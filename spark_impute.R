library(sparklyr)
library(dplyr)
library(tibble)
library(dbplyr)

spark_disconnect_all()

spark_config()
# spark_install(version = "2.4")
spark_con = spark_connect(master = "local")


input_data = copy_to(
  spark_con, 
  missRanger::generateNA(nycflights13::flights , p = 0.2) ,
  "flights" , 
  overwrite = T)

nas_to_level = function( input_data ){
  
  suppressMessages(
    cat_cols = input_data %>% 
      head(10) %>% 
      collect() %>% sapply( function(x) is.character(x) | is.factor(x) )
  )
  cat_cols = names(cat_cols)[cat_cols]
  cat('Filling columns:\n\t' , cat_cols , '\n')
  
  input_data %>% 
    mutate_at( cat_cols , ~ ifelse(is.na(.), "na_level" , . )  ) 
  
}

impute_spark = function( input_data , formula = . ~ . , pmm.k = 3L , miniter = 2L , maxiter = 10L  ){
  
  input_data_remote_name = remote_name(input_data) %>% 
    as.character()
  
  input_data %>% 
    mutate_if( is.integer , as.numeric ) ->
    input_data
  
  valid_types = c(
    "DoubleType" , 'IntegerType' , 'StringType' , 'BooleanType'
  )
  
  transp = function(spark_df){
    spark_df %>% 
      collect() %>% 
      t() %>% 
      as.data.frame() %>% 
      rownames_to_column() %>% 
      arrange(desc(V1))
  }
  
  data_nrow = sdf_nrow(input_data)
  
  input_data %>%
    summarise_all(~sum(as.integer(is.na(.)), na.rm = T)) %>% 
    transp() %>% 
    arrange(V1) ->
    missigness_counts
  
  input_data %>% 
    summarise_all( n_distinct ) %>% 
    transp() ->
    distinct_counts
  
  missigness_counts %>% 
    filter(V1 > 0 & V1 < data_nrow ) %>% 
    pull(rowname) ->
    cols_to_impute_by_na_count
  
  missigness_counts %>% 
    filter(V1 < data_nrow ) %>% 
    pull(rowname) ->
    imputation_cols
  
  data_schema = sdf_schema(input_data)
  lapply(data_schema, function(this_col){
    this_col$type
  })  %>% bind_rows() %>% 
    transp() %>% 
    mutate( 
      task = case_when(
        V1 %in% c('IntegerType' , 'DoubleType') ~ 'Regression' ,
        V1 %in% c('StringType' , 'BooleanType') ~ 'Classification' , 
        TRUE ~ 'None'
      )) ->
    column_type_df
  
  column_type_df %>% 
    filter( V1 %in% valid_types ) %>% 
    pull(rowname) ->
    valid_cols_by_type
  
  column_type_df %>% 
    filter( !V1 %in% valid_types ) %>% 
    pull(rowname) ->
    invalid_cols_by_type
  
  cols_to_impute = cols_to_impute_by_na_count[cols_to_impute_by_na_count %in% valid_cols_by_type]
  imputation_cols = imputation_cols[imputation_cols %in% valid_cols_by_type ]
  completed_cols = setdiff( imputation_cols , cols_to_impute)
  
  shadow_data = input_data %>% 
    select(!!cols_to_impute) %>% 
    mutate_all( is.na ) %>% 
    mutate_all(as.integer)
  
  
  iteration = 1L
  continue = T
  prediction_error = setNames( rep(1, length(cols_to_impute)), cols_to_impute)
  
  while ( continue && iteration <= maxiter   ) {
    
    cat('iteration:' , iteration , '\n\t' )
    latest_data = input_data
    latest_prediction_error = prediction_error
    
    for ( this_col in cols_to_impute) {
      
      cat( this_col ,  ',' , sep='' )  
      
      if ( length(completed_cols) == 0L) {
        
        this_col_out = paste0(this_col , '_out')
        input_data %>% 
          ft_imputer( input_cols = this_col , output_cols = this_col_out ) %>% 
          select(-this_col) %>% 
          rename(!!this_col := !!this_col_out ) %>% 
          compute(input_data_remote_name) ->
          input_data
        
      } else {
        
        this_prediction_task = column_type_df %>% 
          filter( rowname == this_col) %>% 
          pull(task)
        
        suppressMessages(
          input_data %>% 
            select(!!completed_cols , !!this_col ) %>% 
            na.omit() %>% 
            ml_random_forest( formula = reformulate( completed_cols , response = this_col ) ) ->
            this_model
        )
        
        if(this_prediction_task == 'Classification' ){
          this_model %>% 
            ml_predict( input_data %>% select(!!completed_cols  ) ) %>% 
            select(predicted_label) %>% 
            rename(prediction = predicted_label) ->
            prediction_table
          
        } else{
          this_model %>% 
            ml_predict( input_data %>% select(!!completed_cols  ) ) %>% 
            select(prediction)  ->
            prediction_table
        }
        
        prediction_table %>% 
          compute('prediction_table') ->
          prediction_table
        
        this_shadow_col_name = paste0( this_col , '_shadow' , sep='' )
        this_shadow_col_sym = sym(this_shadow_col_name)
        this_prediction_col_name = paste0( this_col , '_pred' , sep='' )
        this_prediction_col_sym = sym(this_prediction_col_name)
        this_col_sym = sym(this_col)
        
        this_shadow = shadow_data %>% 
          select(!!this_col) %>% 
          rename( !!this_shadow_col_name := !!this_col  ) %>% 
          compute('this_shadow')
        
        input_data %>% 
          sdf_bind_cols(this_shadow) %>% 
          sdf_bind_cols(prediction_table) %>% 
          mutate( !!this_prediction_col_name := if_else( !!this_shadow_col_sym  == 1 , prediction ,  this_col_sym ) ) ->
          input_data
        
        input_data %>% 
          filter( this_shadow_col_sym == 0 ) %>% 
          select( !!this_col ,  prediction ) %>% 
          compute('this_evaluation_frame') ->
          evaluation_frame
        
        # evaluate
        
        if(this_prediction_task == 'Classification' ){
          
          evaluation_frame %>% 
            mutate( pred_equal = if_else(prediction == !!this_col_sym , 1 , 0) ) %>% 
            summarise( equal_count = sum(pred_equal , na.rm = TRUE) ) %>% 
            pull(equal_count) ->
            equal_count
          
          evaluation_frame_nrow = sdf_nrow(evaluation_frame)
          
          this_evaluation_metric = equal_count / evaluation_frame_nrow
          
        } else {
          
          evaluation_frame %>% 
            summarise(this_var = var(!!this_col_sym  ) ) %>% 
            pull(this_var) ->
            this_var
          
          evaluation_frame %>% 
            summarise( this_rmse = sqrt(mean((!!this_col_sym - prediction )^2))  ) %>% 
            pull(this_rmse ) ->
            this_rmse
          
          this_evaluation_metric = this_rmse / this_var
          
        }
        
        prediction_error[[this_col]] = this_evaluation_metric
        
        input_data %>% 
          select(-!!this_shadow_col_sym , -prediction , -!!this_col ) %>% 
          rename( !!this_col := !!this_prediction_col_name ) %>% 
          compute(input_data_remote_name) ->
          input_data
        
      }
      
      if ( iteration == 1L && ( this_col %in% imputation_cols ) ) {
        completed_cols = union( completed_cols, this_col )
      }
    }
    
    continue = mean(prediction_error) < mean(latest_prediction_error) | iteration <= miniter
    iteration = iteration + 1L
    cat('\nLatest prediction error mean:' , mean( prediction_error ) ,'\n' )
    cat('Previous prediction error mean:' , mean( latest_prediction_error ) ,'\n' )
    cat('Prediction errors:\n')
    print(prediction_error)
    
    if (iteration == 2L || ( iteration == maxiter && continue )) {
      latest_data = input_data
      latest_prediction_error = prediction_error
    }

  }
  
  return( compute(input_data , input_data_remote_name) )
}

input_data %>% 
  nas_to_level() %>% 
  impute_spark()

import dlt; 


p = dlt.pipeline(pipeline_name='test_conn', destination='snowflake', dataset_name='test_ds'); 
p.sync_destination(); 
print(' KẾT NỐI THÀNH CÔNG TỚI SNOWFLAKE!')
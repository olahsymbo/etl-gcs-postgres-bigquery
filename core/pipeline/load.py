



# Define BigQuery connection parameters
bq_project = 'your_bq_project_name'
bq_dataset = 'your_bq_dataset_name'
bq_table_name = 'processed_data'
bq_credentials_file_path = 'path/to/your/credentials.json'

# Connect to BigQuery and push merged data to table
bq_client = bigquery.Client.from_service_account_json(bq_credentials_file_path)
bq_table_ref = bq_client.dataset(bq_dataset).table(bq_table_name)

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

job = bq_client.load_table_from_dataframe(merged_df, bq_table_ref, job_config=job_config)
job.result()

print('ETL pipeline complete!')
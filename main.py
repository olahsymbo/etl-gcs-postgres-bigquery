# Import necessary libraries
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import psycopg2
import os

# Define connection parameters for Postgres database
pg_host = 'your_pg_host'
pg_port = 'your_pg_port'
pg_dbname = 'your_pg_dbname'
pg_user = 'your_pg_username'
pg_password = 'your_pg_password'

# Connect to Postgres database and retrieve data
pg_conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_dbname, user=pg_user, password=pg_password)
pg_cursor = pg_conn.cursor()
pg_cursor.execute('SELECT * FROM your_table_name')
pg_data = pg_cursor.fetchall()

# Convert Postgres data to pandas dataframe
pg_df = pd.DataFrame(pg_data, columns=['customer_id', 'pg_column1', 'pg_column2'])

# Define path to CSV files
csv_path = '/your/csv/folder/path'

# Merge CSV files into pandas dataframe
csv_files = [os.path.join(csv_path, f) for f in os.listdir(csv_path) if f.endswith('.csv')]
csv_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Define Google Storage connection parameters
gs_bucket_name = 'your_gs_bucket_name'
gs_json_file_name = 'your_gs_json_file_name'
gs_credentials_file_path = 'path/to/your/credentials.json'

# Connect to Google Storage and retrieve data
storage_client = storage.Client.from_service_account_json(gs_credentials_file_path)
gs_bucket = storage_client.get_bucket(gs_bucket_name)
gs_blob = gs_bucket.blob(gs_json_file_name)
gs_json_data = gs_blob.download_as_string()

# Convert Google Storage data to pandas dataframe
gs_df = pd.read_json(gs_json_data)

# Merge dataframes based on customer_id
merged_df = pd.merge(pg_df, csv_df, on='customer_id')
merged_df = pd.merge(merged_df, gs_df, on='customer_id')

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

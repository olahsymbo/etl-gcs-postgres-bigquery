import os
import bigquery

from utils.base_logger import BaseLogger


class Load:

    def __init__(self, transformed_data):
        self.transformed_data = transformed_data
        self.bq_project = os.environ.get('BQ_PROJECT')
        self.bq_dataset = os.environ.get('BQ_DATASET')
        self.bq_table_name = os.environ.get('BQ_TABLE')
        self.bq_credentials_file_path = os.environ.get('BQ_CREDENTIALS')

    def loader(self):
        bq_client = bigquery.Client.from_service_account_json(self.bq_credentials_file_path)
        bq_table_ref = bq_client.dataset(self.bq_dataset).table(self.bq_table_name)

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job = bq_client.load_table_from_dataframe(self.transformed_data, bq_table_ref, job_config=job_config)
        job.result()

        return None

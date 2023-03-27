import os
import psycopg2
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


class ConnectDB:
    def __init__(self):
        self.pg_host = os.environ.get('PG_HOST')
        self.pg_port = os.environ.get('PG_PORT')
        self.pg_dbname = os.environ.get('PG_DBNAME')
        self.pg_user = os.environ.get('PG_USERNAME')
        self.pg_password = os.environ.get('PG_PASSWORD')

        self.gs_json_bucket_name = os.environ.get('GCS_JSON_BUCKET_NAME')
        self.gs_json_file_name = os.environ.get('GCS_JSON_FILENAME')
        self.gs_credentials_file_path = os.environ.get('GCS_CREDENTIAL_PATH')
        self.gs_csv_bucket_name = os.environ.get('GCS_CSV_BUCKET_NAME')
        self.gs_csv_file_name = os.environ.get('GCS_CSV_FILENAME')

    def pg_connect(self):
        pg_conn = psycopg2.connect(host=self.pg_host, port=self.pg_port,
                                   dbname=self.pg_dbname, user=self.pg_user, password=self.pg_password)
        pg_cursor = pg_conn.cursor()
        return pg_cursor

    def gcs_connect(self):
        storage_client = storage.Client.from_service_account_json(self.gs_credentials_file_path)
        gs_blobs = storage_client.list_blobs(self.gs_json_bucket_name, prefix='json/')
        return gs_blobs

    def csv_connect(self):
        storage_client = storage.Client.from_service_account_json(self.gs_credentials_file_path)
        csv_blob = storage_client.list_blobs(self.gs_csv_bucket_name, prefix='csv/')
        return csv_blob
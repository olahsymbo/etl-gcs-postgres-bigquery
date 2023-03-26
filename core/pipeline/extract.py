import os

import pandas as pd
from helpers.db import gcs_connect, csv_connect, pg_connect


class Extract:

    @staticmethod
    def pg_extract():

        pg_connect.execute('SELECT * FROM your_table_name')
        pg_data = pg_connect.fetchall()

        pg_df = pd.DataFrame(pg_data, columns=['customer_id', 'pg_column1', 'pg_column2'])
        return pg_df

    @staticmethod
    def csv_extract():

        gs_csv_data = csv_connect.download_as_string()
        csv_files = [f for f in gs_csv_data if f.endswith('.csv')]
        csv_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        return csv_df

    @staticmethod
    def gcs_extract():

        gs_json_data = gcs_connect.download_as_string()
        gs_df = pd.read_json(gs_json_data)
        return gs_df
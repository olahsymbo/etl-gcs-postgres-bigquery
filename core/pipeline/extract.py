import datetime
import json

import pandas as pd
from helpers.db import gcs_connect, csv_connect, pg_connect


class Extract:

    def __init__(self):
        self.end_date = datetime.date.today()
        self.start_date = self.end_date - datetime.timedelta(days=7)

    def pg_extract(self):

        pg_connect.execute("SELECT * FROM customer WHERE created_at >= %s AND created_at <= %s",
                           (self.start_date, self.end_date))
        pg_data = pg_connect.fetchall()

        pg_df = pd.DataFrame(pg_data)
        return pg_df

    def csv_extract(self):
        csv_files = []
        for blob in csv_connect:
            if blob.name.endswith('.csv'):
                blob_date = blob.updated.date()
                if self.start_date <= blob_date <= self.end_date:
                    csv_files.append(blob)
        csv_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        return csv_df

    def gcs_extract(self):
        json_data_list = []
        for blob in gcs_connect:
            if blob.name.endswith('.json'):
                blob_date = blob.updated.date()
                if self.start_date <= blob_date <= self.end_date:
                    content = blob.download_as_string().decode('utf-8')
                    json_data = json.loads(content)
                    json_data_list.append(json_data)

        gs_df = pd.concat([pd.DataFrame(data) for data in json_data_list], ignore_index=True)
        return gs_df

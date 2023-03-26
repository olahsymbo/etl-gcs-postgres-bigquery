import pandas as pd


class Transform:

    MERGE_KEY = 'customer_id'
    def __init__(self, data):
        self.data = data

    def merge_transform(self, **kwargs):

        for key, value in kwargs.items():
            if isinstance(value, pd.DataFrame):
                # Do some processing on the dataframe
                data1 = value.dropna()
                print(f"Processed dataframe {key}:")
                print(processed_data)

        pg_df = kwargs.get()
        merged_df = pd.merge(pg_df, csv_df, on='customer_id')
        merged_df = pd.merge(merged_df, gs_df, on='customer_id')

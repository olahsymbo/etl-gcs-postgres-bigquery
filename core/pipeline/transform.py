import pandas as pd


class Transform:

    MERGE_KEY = 'customer_id'
    def __init__(self, **kwargs):
        self.df1 = kwargs.get('df1')
        self.df2 = kwargs.get('df2')
        self.df3 = kwargs.get('df3')

    def merge_transform(self):
        merged_df = pd.merge(self.df1, self.df2, on='customer_id')
        merged_df = pd.merge(merged_df, self.df3, on='customer_id')

        return merged_df

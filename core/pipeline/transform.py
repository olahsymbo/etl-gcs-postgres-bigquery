import pandas as pd


class Transform:
    MERGE_KEY = 'customer_id'

    def __init__(self, df1, df2, df3):
        self.df1 = df1
        self.df2 = df2
        self.df3 = df3

    def merge_transform(self):
        merged_df = pd.merge(self.df1, self.df2, on=self.MERGE_KEY)
        merged_df = pd.merge(merged_df, self.df3, on=self.MERGE_KEY)
        return merged_df

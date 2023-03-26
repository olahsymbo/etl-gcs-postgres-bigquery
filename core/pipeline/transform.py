




# Merge dataframes based on customer_id
merged_df = pd.merge(pg_df, csv_df, on='customer_id')
merged_df = pd.merge(merged_df, gs_df, on='customer_id')

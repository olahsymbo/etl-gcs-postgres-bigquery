from helpers.db.connection import pg_cursor


class Extract:

    def __init__(self):
        self.pg_cursor =

pg_cursor.execute('SELECT * FROM your_table_name')
pg_data = pg_cursor.fetchall()

# Convert Postgres data to pandas dataframe
pg_df = pd.DataFrame(pg_data, columns=['customer_id', 'pg_column1', 'pg_column2'])


# Define path to CSV files
csv_path = '/your/csv/folder/path'

# Merge CSV files into pandas dataframe
csv_files = [os.path.join(csv_path, f) for f in os.listdir(csv_path) if f.endswith('.csv')]
csv_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)



gs_json_data = gs_blob.download_as_string()

# Convert Google Storage data to pandas dataframe
gs_df = pd.read_json(gs_json_data)
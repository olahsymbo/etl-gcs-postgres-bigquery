from connection import ConnectDB

connect_db = ConnectDB()

pg_connect = connect_db.pg_connect()
gcs_connect = connect_db.gcs_connect()
csv_connect = connect_db.csv_connect()
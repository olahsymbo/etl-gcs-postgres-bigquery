# ETL-GCS-POSTGRES-BIGQUERY

A project to run ETL on multiple data sources (postgres DB, csvs, jsons), transform the data, and push the transformed data to BigQuery


### Getting Started 

To enable this app function properly, install the dependencies using `poetry`.

Simply run:

```
poetry shell
poetry install
```

Ensure the python version is `3.8`

Once all the dependencies have been installed, proceed as follows:

- Create the `.env` and insert the necessary credentials
- Launch the ETL flask server, which will also create a scheduler on Google Scheduler platform

The details of the steps are provided as follows:

### Create a Postgres DB

The following credentials need to be provided in the `.env` file:

```
PG_HOST=
PG_PORT=
PG_DBNAME=
PG_USERNAME=
PG_PASSWORD=

GCS_JSON_BUCKET_NAME=
GCS_JSON_FILENAME=
GCS_CREDENTIAL_PATH=
CSV_PATH=

GCS_CSV_BUCKET_NAME=
GCS_CSV_FILENAME=

BQ_CREDENTIALS=
BQ_PROJECT=
BQ_TABLE=
BQ_DATASET=

GOOGLE_SCHEDULER_PROJECT_ID=

FLASK_HOST=
```

My postgres DB is in RDS. However, incase you want your postgres DB on your local PC, you can follow the README.md in this [repo](https://github.com/olahsymbo/data-extraction-service) to guide you on how to create Postgres DB and insert records.

For json and csv records, I used Google Cloud Storage ([GCS](https://cloud.google.com/storage)).

### Launch ETL server

The main app is `app.py`. It contains the api running the ETL pipeline and a job scheduler which runs on Google Cloud Scheduler

- Again launch the server using:

    `python app.py`

- to start the process page, you can make a curl request:

    `http://127.0.0.1:8000/run_pipeline`
 
Normally, `127.0.0.1` will be changed to the domain url. 
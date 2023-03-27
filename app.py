import os
import traceback

from flask import Flask, jsonify
from google.cloud import scheduler

from core.pipeline import extract, transform, load
from utils.base_logger import BaseLogger


HOME = os.path.dirname(os.path.abspath(__file__))

logs_path = os.path.join(HOME, 'logs')
logger_obj = BaseLogger("etl_log", log_file=logs_path)


app = Flask(__name__)
FLASK_HOST = os.environ.get('FLASK_HOST')

if not os.path.exists(logs_path):
    os.makedirs(logs_path, exist_ok=True)


def run_pipeline():
    extract_obj = extract.Extract()
    pg_data = extract_obj.pg_extract()
    csv_data = extract_obj.csv_extract()
    json_data = extract_obj.gcs_extract()

    transform_obj = transform.Transform(df1=pg_data, df2=csv_data, df3=json_data)
    transformed_data = transform_obj.merge_transform()

    load_obj = load.Load(transformed_data)
    load_obj.loader()
    return transformed_data


@app.route('/run-pipeline')
def run_pipeline_endpoint():
    try:
        transformed_data = run_pipeline()
        json_str = transformed_data.head(5).to_json(orient='records')
        logger_obj.logger.info(["ETL Task Completed!"])
        return jsonify({'data': json_str,
                        'message': 'ETL pipeline complete!',
                        'error': None,
                        'status': 200}), 200
    except Exception:
        logger_obj.logger.error([traceback.format_exc()])
        return jsonify({'message': 'ETL pipeline failed!',
                        'error': 'Bad Request',
                        'status': 400}), 400



# Define function to create and schedule Google Cloud Scheduler job
def create_scheduler_job():

    client = scheduler.CloudSchedulerClient()
    parent = client.location_path(os.environ.get('GOOGLE_SCHEDULER_PROJECT_ID'), 'your_location')
    job_name = 'etl-job'
    target_url = FLASK_HOST + '/run-pipeline'
    schedule = '0 5 * * 1'  # schedule for Mondays at 5am
    time_zone = 'America/Los_Angeles'
    payload = ''
    headers = {'Content-Type': 'application/json'}
    body = {
        'http_target': {
            'uri': target_url,
            'http_method': 'GET',
            'headers': headers,
            'body': payload
        },
        'schedule': schedule,
        'time_zone': time_zone
    }
    job = client.create_job(parent=parent, job=body, job_id=job_name)
    print(f'Created job {job.name} in project {job.project_id}')


# Create and run scheduler
create_scheduler_job()


if __name__ == '__main__':
    app.run(debug=True, port=8000)

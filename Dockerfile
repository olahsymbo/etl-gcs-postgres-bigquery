FROM python:3.8-slim
ENV PYTHONPATH=/usr/lib/python3.8/site-packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends apt-utils \
    && apt-get install -y git \ 
    python3-pip \
    && apt-get clean \
    && apt-get autoremove 
RUN mkdir /etl-gcs-postgres-bigquery
WORKDIR /etl-gcs-postgres-bigquery
COPY . /etl-gcs-postgres-bigquery   
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root
EXPOSE 8050
CMD exec gunicorn --bind :$PORT --workers 4 --threads 8 wsgi:application

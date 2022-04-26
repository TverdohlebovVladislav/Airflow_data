FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

# default credentials: login - airflow, password - airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --upgrade google-api-python-client oauth2client

WORKDIR $AIRFLOW_HOME

COPY dags dags
COPY example example
COPY csv csv
COPY data_generate_scripts data_generate_scripts

COPY scripts scripts
RUN chmod +x scripts

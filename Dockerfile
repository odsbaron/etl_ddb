FROM apache/airflow:2.9.0-python3.10
USER root
RUN apt-get update && apt-get install -y gcc
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ /opt/airflow/config/
COPY etl/ /opt/airflow/etl/
COPY dags/ /opt/airflow/dags/

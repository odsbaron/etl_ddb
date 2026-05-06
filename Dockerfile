FROM apache/airflow:2.9.0-python3.10
USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean
RUN mkdir -p /opt/airflow/checkpoint && chown airflow:root /opt/airflow/checkpoint
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ /opt/airflow/config/
COPY etl/ /opt/airflow/etl/
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/

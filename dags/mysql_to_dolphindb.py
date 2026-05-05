from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl.pipeline import ETLPipeline
from etl.checkpoint import CheckpointManager
import yaml
import os

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "mysql_to_dolphindb_etl",
    default_args=default_args,
    description="MySQL to DolphinDB incremental sync",
    schedule_interval="@hourly",
    catchup=False,
)

config_path = os.environ.get(
    "ETL_CONFIG_PATH", "/opt/airflow/config/config.yaml"
)
with open(config_path) as f:
    config = yaml.safe_load(f)

checkpoint_mgr = CheckpointManager(config["checkpoint"]["db_path"])


def create_etl_task(job_conf):
    def run_etl(**context):
        pipeline = ETLPipeline(
            job_conf,
            config["mysql"],
            config["dolphindb"],
            checkpoint_mgr,
        )
        pipeline.run()

    return run_etl


for job in config["jobs"]:
    PythonOperator(
        task_id=f"etl_{job['name']}",
        python_callable=create_etl_task(job),
        dag=dag,
    )

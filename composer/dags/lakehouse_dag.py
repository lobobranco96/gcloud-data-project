from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"
CLUSTER_NAME = "lobobranco-dataproc"

BUCKET = "gs://lakehouse_lb_bucket"
RAW_PATH = f"{BUCKET}/raw"
BRONZE_PATH = f"{BUCKET}/bronze"
SILVER_PATH = f"{BUCKET}/silver"
GOLD_PATH = f"{BUCKET}/gold"

BRONZE_SCRIPT = "gs://lakehouse_lb_bucket/scripts/bronze.py"
SILVER_SCRIPT = "gs://lakehouse_lb_bucket/scripts/silver.py"
GOLD_SCRIPT = "gs://lakehouse_lb_bucket/scripts/gold.py"

default_args = {
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pipeline_delta_medallion",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dataproc"],
    start_date=datetime(2025, 6, 9, tzinfo=timezone("America/Sao_Paulo"))
) as dag:

    executar_bronze = DataprocSubmitJobOperator(
        task_id="executar_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": BRONZE_SCRIPT,
                "args": [
                    "--raw_path={{ params.raw_path }}/2025-06-11",#{{ ds }}",
                    "--bronze_path={{ params.bronze_path }}",
                    "--ingest_date=2025-06-11",#{{ ds }}",
                ],
            "jar_file_uris": [
                "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
                "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
            ]
            },
        },
        params={
            "raw_path": RAW_PATH,
            "bronze_path": BRONZE_PATH,
        },
    )

    executar_silver = DataprocSubmitJobOperator(
        task_id="executar_silver",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": SILVER_SCRIPT,
                "args": [
                    "--bronze_path={{ params.bronze_path }}",
                    "--silver_path={{ params.silver_path }}",
                    "--ingest_date={{ ds }}",
                ],
            "jar_file_uris": [
                "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
                "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
            ]
            },
        },
        params={
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
        },
    )

    executar_gold = DataprocSubmitJobOperator(
        task_id="executar_gold",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": GOLD_SCRIPT,
                "args": [
                    "--silver_path={{ params.silver_path }}",
                    "--gold_path={{ params.gold_path }}",
                    "--ingest_date={{ ds }}",
                ],
                "jar_file_uris": [
                "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
                "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
            ]
            },
        },
        params={
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
        },
    )

    executar_bronze >> executar_silver >> executar_gold

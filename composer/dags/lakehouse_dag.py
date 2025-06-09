from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"
CLUSTER_NAME = "lobobranco-dataproc"

BUCKET = "gs://lakehouse_lb_bucket"  # ajuste seu bucket
RAW_PATH = f"{BUCKET}/raw"
BRONZE_PATH = f"{BUCKET}/bronze"
SILVER_PATH = f"{BUCKET}/silver"
GOLD_PATH = f"{BUCKET}/gold"

# Caminhos dos scripts PySpark no bucket
BRONZE_SCRIPT = "gs://lakehouse_lb_bucket/scripts/bronze.py"
SILVER_SCRIPT = "gs://lakehouse_lb_bucket/scripts/silver.py"
GOLD_SCRIPT = "gs://lakehouse_lb_bucket/scripts/gold.py"

default_args = {
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pipeline_lakehouse",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dataproc"]
) as dag:

    criar_cluster = DataprocCreateClusterOperator(
        task_id="criar_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    cluster_config={
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_size_gb": 50
            }
        },
        "worker_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_size_gb": 50
            }
        },
        "software_config": {
            "properties": {
                # Delta Lake 2.2.0 para Spark 3.3.x e Scala 2.12
                "spark.jars.packages": "io.delta:delta-core_2.12:2.2.0",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        },
        "gce_cluster_config": {
            "service_account": "data-pipeline-sa@lobobranco-458901.iam.gserviceaccount.com"
        }
    }
)

    executar_bronze = DataprocSubmitJobOperator(
        task_id="executar_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": BRONZE_SCRIPT,
                "args": [
                    f"--raw_path={RAW_PATH}/{{{{ ds }}}}",
                    f"--bronze_path={BRONZE_PATH}",
                    f"--ingest_date={{{{ ds }}}}",
                ],
            },
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
                    f"--bronze_path={BRONZE_PATH}",
                    f"--silver_path={SILVER_PATH}",
                    f"--ingest_date={{{{ ds }}}}",
                ],
            },
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
                    f"--silver_path={SILVER_PATH}",
                    f"--gold_path={GOLD_PATH}",
                    f"--ingest_date={{{{ ds }}}}",
                ],
            },
        },
    )

    deletar_cluster = DataprocDeleteClusterOperator(
        task_id="deletar_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,  # roda mesmo que algo falhe
    )

    # Definindo a ordem das tasks
    criar_cluster >> executar_bronze >> executar_silver >> executar_gold >> deletar_cluster

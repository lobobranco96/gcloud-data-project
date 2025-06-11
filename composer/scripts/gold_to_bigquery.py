from pyspark.sql import SparkSession
from pyspark import SparkConf
import argparse
import logging


def create_spark_session():
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = SparkSession.builder \
        .appName("ExportGoldToBigQuery") \
        .config(conf=conf) \
        .getOrCreate()

    return spark


def export_to_bigquery(logger, df, table_name):
    try:
        logger.info(f"Exportando dados para BigQuery: {table_name}")
        df.write \
            .format("bigquery") \
            .option("table", f"lobobranco-458901.lakehouse.{table_name}") \
            .option("temporaryGcsBucket", "lakehouse_lb_bucket/lakehouse_data/tmp") \
            .mode("append") \
            .save()
        logger.info(f"Exportação finalizada: {table_name}")
    except Exception as e:
        logger.error(f"Erro ao exportar {table_name} para BigQuery: {e}")


def main(spark, gold_path, ingest_date):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("ExportGoldToBigQuery")

    logger.info("Iniciando exportação da camada Gold para BigQuery")

    # Carrega os diretórios principais e aplica filtro por ingest_date
    fact_sales = spark.read.format("delta") \
        .load(f"{gold_path}/fact_sales") \
        .where(f"ingest_date = '{ingest_date}'")

    dim_product = spark.read.format("delta") \
        .load(f"{gold_path}/dim_products") \
        .where(f"ingest_date = '{ingest_date}'")

    current_inventory = spark.read.format("delta") \
        .load(f"{gold_path}/current_inventory") \
        .where(f"ingest_date = '{ingest_date}'")

    export_to_bigquery(logger, fact_sales, "fact_sales")
    export_to_bigquery(logger, dim_product, "dim_product")
    export_to_bigquery(logger, current_inventory, "current_inventory")

    logger.info("Exportação para BigQuery finalizada com sucesso")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold_path", required=True)
    parser.add_argument("--ingest_date", required=True)
    args = parser.parse_args()

    spark = create_spark_session()
    main(spark, args.gold_path, args.ingest_date)

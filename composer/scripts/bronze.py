import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 📋 Configuração do Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bronze_layer")

def read_csv_with_validation(spark, path, ingest_date):
    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("badRecordsPath", f"/tmp/bad_records/{ingest_date}")
            .csv(path)
        )
        df = df.withColumn("ingest_date", lit(ingest_date))
        logger.info(f"Leitura concluída para {path} com {df.count()} registros.")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler {path}: {e}")
        return None

def main(raw_path, bronze_path, ingest_date):
    spark = SparkSession.builder \
        .appName("BronzeLayer") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars", "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.2.0.jar,gs://lakehouse_lb_bucket/jars/delta-storage-2.2.0.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger.info("Iniciando ingestão na camada Bronze...")

    tables = ["orders", "customers", "products", "order_items", "inventory_updates"]

    for table in tables:
        file_path = f"{raw_path}/{table}.csv"
        logger.info(f"Lendo a tabela {table} de {file_path}")
        df = read_csv_with_validation(spark, file_path, ingest_date)

        if df:
            output_path = f"{bronze_path}/{table}"
            try:
                df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("ingest_date") \
                    .save(output_path)
                logger.info(f"Dados gravados com sucesso em {output_path}")
            except Exception as e:
                logger.error(f"Erro ao gravar Delta para {table}: {e}")
        else:
            logger.warning(f"Tabela {table} não foi processada.")

    spark.stop()
    logger.info("Ingestão Bronze finalizada com sucesso.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_path", required=True)
    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--ingest_date", required=True)
    args = parser.parse_args()

    main(args.raw_path, args.bronze_path, args.ingest_date)

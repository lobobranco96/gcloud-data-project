from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import argparse
import logging

def main(bronze_path, silver_path, ingest_date):
    # Configuração do logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("SilverLayer")

    logger.info("Iniciando transformação para camada Silver")

    spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

    try:
        # Carregar dados da camada Bronze filtrando por ingest_date
        logger.info("Lendo dados da camada Bronze...")
        orders = spark.read.format("delta").load(f"{bronze_path}/orders").filter(col("ingest_date") == ingest_date)
        customers = spark.read.format("delta").load(f"{bronze_path}/customers").filter(col("ingest_date") == ingest_date)
        order_items = spark.read.format("delta").load(f"{bronze_path}/order_items").filter(col("ingest_date") == ingest_date)

        # Transformações - correção de tipos e filtros
        logger.info("Transformando e limpando dados...")
        orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd HH:mm:ss"))
        orders = orders.filter(col("status").isin(["completed", "processing"]))

        # Remover duplicados no pedido (order_id)
        orders = orders.dropDuplicates(["order_id"])

        # Join para enriquecer dados
        logger.info("Realizando join para criar order_details...")
        order_details = (
            order_items.join(orders, "order_id", "inner")
                       .join(customers, "customer_id", "inner")
                       .select(
                           "order_id",
                           "order_date",
                           "status",
                           "customer_id",
                           "name",
                           "email",
                           "product_id",
                           "quantity",
                           "unit_price"
                       )
        )

        # Escrita na camada Silver
        logger.info("Salvando dados transformados na camada Silver...")
        order_details.write.format("delta") \
            .mode("append") \
            .partitionBy("order_date") \
            .save(f"{silver_path}/order_details")

        logger.info("✅ Silver finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante processamento da camada Silver: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--ingest_date", required=True)
    args = parser.parse_args()

    main(args.bronze_path, args.silver_path, args.ingest_date)

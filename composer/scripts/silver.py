from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, to_date, lit
import argparse
import logging
from datetime import datetime

def create_spark_session():
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = SparkSession.builder \
        .appName("SilverDelta") \
        .config(conf=conf) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    return spark

def process_customers(logger, spark, bronze_path, silver_path, ingest_date):
    try:
        logger.info("Lendo dados da camada Bronze - customers...")
        customers = spark.read.format("delta") \
            .load(f"{bronze_path}/customers") \
            .filter(col("ingest_date") == ingest_date)

        logger.info("Transformando e limpando dados dos clientes...")
        customers_cleaned = (
            customers
            .dropDuplicates(["customer_id"])
            .filter(col("email").isNotNull())
            .filter(col("created_at") <= to_date(lit(ingest_date)))
            .withColumn("address", col("address").trim())
        )

        logger.info("Salvando clientes limpos na camada Silver...")
        customers_cleaned.write.format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(f"{silver_path}/customers_cleaned")
    except Exception as e:
        logger.error(f"Erro ao processar customers: {e}")

def process_products(logger, spark, bronze_path, silver_path, ingest_date):
    try:
        logger.info("Lendo dados da camada Bronze - products...")
        products = spark.read.format("delta") \
            .load(f"{bronze_path}/products") \
            .filter(col("ingest_date") == ingest_date)

        logger.info("Transformando e limpando dados dos produtos...")
        products_cleaned = (
            products
            .dropDuplicates(["product_id"])
            .filter(col("price") > 0)
            .filter(col("category") != "")
        )

        logger.info("Salvando produtos limpos na camada Silver...")
        products_cleaned.write.format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(f"{silver_path}/products_cleaned")
    except Exception as e:
        logger.error(f"Erro ao processar products: {e}")

def process_inventory_updates(logger, spark, bronze_path, silver_path, ingest_date):
    try:
        logger.info("Lendo dados da camada Bronze - inventory_updates...")
        inventory = spark.read.format("delta") \
            .load(f"{bronze_path}/inventory_updates") \
            .filter(col("ingest_date") == ingest_date)

        logger.info("Transformando e limpando dados de inventário...")
        inventory_cleaned = (
            inventory
            .filter(col("change") != 0)
            .filter(col("timestamp") <= lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        )

        logger.info("Salvando inventário limpo na camada Silver...")
        inventory_cleaned.write.format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(f"{silver_path}/inventory_cleaned")
    except Exception as e:
        logger.error(f"Erro ao processar inventory_updates: {e}")

def process_orders(logger, spark, bronze_path, silver_path, ingest_date):
    try:
        logger.info("Lendo dados da camada Bronze - orders...")
        orders = spark.read.format("delta") \
            .load(f"{bronze_path}/orders") \
            .filter(col("ingest_date") == ingest_date)

        logger.info("Transformando e limpando dados dos pedidos...")
        orders_cleaned = (
            orders
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd HH:mm:ss"))
            .filter(col("status").isin(["completed", "processing"]))
            .dropDuplicates(["order_id"])
        )

        logger.info("Salvando pedidos limpos na camada Silver...")
        orders_cleaned.write.format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(f"{silver_path}/orders_cleaned")

    except Exception as e:
        logger.error(f"Erro ao processar orders: {e}")

def process_order_items(logger, spark, bronze_path, silver_path, ingest_date):
    try:
        logger.info("Lendo dados da camada Bronze - order_items...")
        items = spark.read.format("delta") \
            .load(f"{bronze_path}/order_items") \
            .filter(col("ingest_date") == ingest_date)

        logger.info("Transformando e limpando dados dos itens dos pedidos...")
        items_cleaned = (
            items
            .filter(col("quantity") > 0)
            .filter(col("unit_price") > 0)
            .withColumn("total_price", col("quantity") * col("unit_price"))
        )

        logger.info("Salvando itens de pedido limpos na camada Silver...")
        items_cleaned.write.format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(f"{silver_path}/order_items_cleaned")

    except Exception as e:
        logger.error(f"Erro ao processar order_items: {e}")

def main(spark, bronze_path, silver_path, ingest_date):
    # Configuração do logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("SilverLayer")

    logger.info("Iniciando o processamento da camada Silver.")

    process_customers(logger, spark, bronze_path, silver_path, ingest_date)
    process_inventory_updates(logger, spark, bronze_path, silver_path, ingest_date)
    process_order_items(logger, spark, bronze_path, silver_path, ingest_date)
    process_orders(logger, spark, bronze_path, silver_path, ingest_date)
    process_products(logger, spark, bronze_path, silver_path, ingest_date)

    spark.stop()
    logger.info("Processamento da camada Silver finalizado com sucesso.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--ingest_date", required=True)
    args = parser.parse_args()

    spark = create_spark_session()
    main(spark, args.bronze_path, args.silver_path, args.ingest_date)

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, sum as _sum, max as _max, lit
import argparse
import logging

def create_spark_session():
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = SparkSession.builder \
        .appName("GoldDelta") \
        .config(conf=conf) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    return spark

def build_fact_sales(logger, spark, silver_path, gold_path, ingest_date):
    try:
        logger.info("Lendo dados da Silver: orders e order_items")
        orders = spark.read.format("delta").load(f"{silver_path}/orders_cleaned")
        order_items = spark.read.format("delta").load(f"{silver_path}/order_items_cleaned")

        logger.info("Construindo tabela de fato: fact_sales")
        fact_sales = orders.join(order_items, on="order_id", how="inner") \
            .select(
                "order_id", "customer_id", "product_id", "order_date",
                "quantity", "unit_price", "total_price", "status"
            ) \
            .withColumn("ingest_date", lit(ingest_date))

        logger.info("Escrevendo fact_sales na Gold com particionamento por ingest_date")
        fact_sales.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .save(f"{gold_path}/fact_sales")

    except Exception as e:
        logger.error(f"Erro ao construir fact_sales: {e}")

def build_dim_customers(logger, spark, silver_path, gold_path, ingest_date):
    try:
        logger.info("Lendo dados da Silver: customers")
        customers = spark.read.format("delta").load(f"{silver_path}/customers_cleaned")

        logger.info("Construindo dimensão: dim_customers")
        dim_customers = customers.select(
            "customer_id", "name", "email", "address", "created_at"
        ).dropDuplicates(["customer_id"]) \
         .withColumn("ingest_date", lit(ingest_date))

        logger.info("Escrevendo dim_customers na Gold com particionamento por ingest_date")
        dim_customers.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .save(f"{gold_path}/dim_customers")

    except Exception as e:
        logger.error(f"Erro ao construir dim_customers: {e}")

def build_dim_products(logger, spark, silver_path, gold_path, ingest_date):
    try:
        logger.info("Lendo dados da Silver: products")
        products = spark.read.format("delta").load(f"{silver_path}/products_cleaned")

        logger.info("Construindo dimensão: dim_products")
        dim_products = products.select(
            "product_id", "name", "category", "price"
        ).dropDuplicates(["product_id"]) \
         .withColumn("ingest_date", lit(ingest_date))

        logger.info("Escrevendo dim_products na Gold com particionamento por ingest_date")
        dim_products.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .save(f"{gold_path}/dim_products")

    except Exception as e:
        logger.error(f"Erro ao construir dim_products: {e}")

def build_current_inventory(logger, spark, silver_path, gold_path, ingest_date):
    try:
        logger.info("Lendo dados da Silver: inventory_cleaned")
        inventory = spark.read.format("delta").load(f"{silver_path}/inventory_cleaned")

        logger.info("Construindo tabela agregada: current_inventory")
        current_inventory = inventory.groupBy("product_id") \
            .agg(
                _sum("change").alias("stock_quantity"),
                _max("timestamp").alias("last_updated")
            ) \
            .withColumn("ingest_date", lit(ingest_date))

        logger.info("Escrevendo current_inventory na Gold com particionamento por ingest_date")
        current_inventory.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .save(f"{gold_path}/current_inventory")

    except Exception as e:
        logger.error(f"Erro ao construir current_inventory: {e}")

def main(spark, silver_path, gold_path, ingest_date):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("GoldLayer")

    logger.info("Iniciando processamento da camada Gold")

    build_fact_sales(logger, spark, silver_path, gold_path, ingest_date)
    build_dim_customers(logger, spark, silver_path, gold_path, ingest_date)
    build_dim_products(logger, spark, silver_path, gold_path, ingest_date)
    build_current_inventory(logger, spark, silver_path, gold_path, ingest_date)

    logger.info("Camada Gold finalizada com sucesso")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--gold_path", required=True)
    parser.add_argument("--ingest_date", required=True, help="Data da partição a ser processada (yyyy-MM-dd)")
    args = parser.parse_args()

    spark = create_spark_session()
    main(spark, args.silver_path, args.gold_path, args.ingest_date)

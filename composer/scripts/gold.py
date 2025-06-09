from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, dayofweek, expr
import argparse

def main(silver_path, gold_path, ingest_date):
    spark = SparkSession.builder \
            .appName("GoldLayer") \
            .getOrCreate()

    # Lê a tabela silver (order_details)
    order_details = spark.read.format("delta") \
        .load(f"{silver_path}/order_details") \
        .filter(col("order_date") == ingest_date)

    # Lê a tabela products da bronze (para montar dimensão produto)
    products = spark.read.format("delta") \
        .load(f"{silver_path}/products") \
        .filter(col("ingest_date") == ingest_date) \
        .dropDuplicates(["product_id"])

    # Lê a tabela customers da silver (ou bronze) para dimensão clientes
    customers = order_details.select("customer_id", "name", "email").dropDuplicates()

    # Dimensão tempo gerada a partir da coluna order_date
    # Extraindo atributos úteis para análises
    time_dim = order_details.select("order_date").dropDuplicates() \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("day", dayofmonth(col("order_date"))) \
        .withColumn("day_of_week", dayofweek(col("order_date"))) \
        .withColumn("quarter", expr("quarter(order_date)"))

    # Tabela fato: juntar detalhes com chave produto e cliente
    fact_sales = order_details.join(products, "product_id", "left") \
                              .join(customers, "customer_id", "left") \
                              .select(
                                  "order_id",
                                  "order_date",
                                  "customer_id",
                                  "product_id",
                                  "quantity",
                                  "unit_price",
                                  "name",      # cliente nome
                                  "email",     # cliente email
                                  "category",  # produto categoria
                                  "price"      # produto preço original
                              )

    # Salvar dimensões e fato como delta (com particionamento na dimensão tempo e na fato)
    customers.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/dim_customers")

    products.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/dim_products")

    time_dim.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/dim_time")

    fact_sales.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .save(f"{gold_path}/fato_vendas")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--gold_path", required=True)
    parser.add_argument("--ingest_date", required=True)
    args = parser.parse_args()

    main(args.silver_path, args.gold_path, args.ingest_date)

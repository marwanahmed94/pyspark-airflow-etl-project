from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, sum as spark_sum,
    year, month, datediff, when, round
)
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data")
OUTPUT_PATH = os.path.join(BASE_DIR, "output")

spark = SparkSession.builder \
    .appName("Retail Data Pipeline - Transform") \
    .getOrCreate()

orders = spark.read.csv(os.path.join(DATA_PATH, "olist_orders_dataset.csv"), header=True, inferSchema=True)
customers = spark.read.csv(os.path.join(DATA_PATH, "olist_customers_dataset.csv"), header=True, inferSchema=True)
products = spark.read.csv(os.path.join(DATA_PATH, "olist_products_dataset.csv"), header=True, inferSchema=True)
order_items = spark.read.csv(os.path.join(DATA_PATH, "olist_order_items_dataset.csv"), header=True, inferSchema=True)
payments = spark.read.csv(os.path.join(DATA_PATH, "olist_order_payments_dataset.csv"), header=True, inferSchema=True)

orders = orders.dropDuplicates().dropna(subset=["order_id", "customer_id"])
customers = customers.dropDuplicates().dropna(subset=["customer_id"])
products = products.dropDuplicates().dropna(subset=["product_id"])
order_items = order_items.dropDuplicates().dropna(subset=["order_id", "product_id"])
payments = payments.dropDuplicates().dropna(subset=["order_id"])

orders = orders.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
orders = orders.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
orders = orders.withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))

payments_total = payments.groupBy("order_id").agg(
    spark_sum("payment_value").alias("total_payment")
)

analytics_df = (
    orders
    .join(customers, "customer_id", "left")
    .join(order_items, "order_id", "left")
    .join(products, "product_id", "left")
    .join(payments_total, "order_id", "left")
)

analytics_df = analytics_df.withColumn("purchase_year", year(col("order_purchase_timestamp")))
analytics_df = analytics_df.withColumn("purchase_month", month(col("order_purchase_timestamp")))

analytics_df = analytics_df.withColumn(
    "delivery_days",
    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
)

analytics_df = analytics_df.withColumn(
    "estimated_delay_days",
    datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
)

analytics_df = analytics_df.withColumn(
    "shipping_ratio",
    when(col("total_payment") > 0, round(col("freight_value") / col("total_payment"), 2)).otherwise(0)
)

analytics_df = analytics_df.select(
    "order_id",
    "customer_id",
    "customer_city",
    "customer_state",
    "order_status",
    "purchase_year",
    "purchase_month",
    "delivery_days",
    "estimated_delay_days",
    "product_id",
    "product_category_name",
    "seller_id",
    "price",
    "freight_value",
    "total_payment",
    "shipping_ratio"
)

analytics_df = analytics_df.dropna(subset=["price", "total_payment"])

sales_by_state = analytics_df.groupBy("customer_state").agg(
    spark_sum("total_payment").alias("total_sales"),
    spark_sum("freight_value").alias("total_freight")
).orderBy(col("total_sales").desc())

sales_by_category = analytics_df.groupBy("product_category_name").agg(
    spark_sum("total_payment").alias("total_sales"),
    spark_sum("price").alias("total_product_price")
).orderBy(col("total_sales").desc())

print("Final analytics rows:", analytics_df.count())

print("Sample analytics data:")
analytics_df.show(5)

print("Sales by state:")
sales_by_state.show(10)

print("Sales by category:")
sales_by_category.show(10)

os.makedirs(OUTPUT_PATH, exist_ok=True)

analytics_df.limit(50000).coalesce(1).write.mode("overwrite").option("header", True).csv(
    os.path.join(OUTPUT_PATH, "retail_analytics")
)

sales_by_state.coalesce(1).write.mode("overwrite").option("header", True).csv(
    os.path.join(OUTPUT_PATH, "sales_by_state")
)

sales_by_category.coalesce(1).write.mode("overwrite").option("header", True).csv(
    os.path.join(OUTPUT_PATH, "sales_by_category")
)

print("Transformation completed successfully.")
print("Folders created:")
print(f"- {os.path.join(OUTPUT_PATH, 'retail_analytics')}")
print(f"- {os.path.join(OUTPUT_PATH, 'sales_by_state')}")
print(f"- {os.path.join(OUTPUT_PATH, 'sales_by_category')}")

spark.stop()
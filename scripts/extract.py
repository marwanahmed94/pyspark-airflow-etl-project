from pyspark.sql import SparkSession
import os
import logging

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data")

DATASETS = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "sellers": "olist_sellers_dataset.csv"
}


def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("Retail Data Pipeline - Extract") \
        .getOrCreate()

    logging.info("Spark session created successfully.")
    return spark


def validate_file_exists(file_path):
    """Check if source file exists before reading."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")


def read_dataset(spark, dataset_name, file_name):
    """Read a CSV dataset using PySpark."""
    file_path = os.path.join(DATA_PATH, file_name)

    validate_file_exists(file_path)

    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    logging.info(f"{dataset_name} dataset loaded successfully.")
    return df


def display_dataset_info(dataset_name, df):
    """Display basic dataset information."""
    print("=" * 60)
    print(f"Dataset: {dataset_name}")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    print("Schema:")
    df.printSchema()
    print("=" * 60)


def main():
    spark = create_spark_session()

    try:
        dataframes = {}

        for dataset_name, file_name in DATASETS.items():
            df = read_dataset(spark, dataset_name, file_name)
            dataframes[dataset_name] = df
            display_dataset_info(dataset_name, df)

        logging.info("Extract phase completed successfully.")

    except Exception as e:
        logging.error(f"Extract phase failed: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
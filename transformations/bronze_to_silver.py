import os
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.spark_session import create_spark_session

# Load environment variables
load_dotenv()

ACCOUNT_NAME = os.getenv("ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

if not all([ACCOUNT_NAME, ACCOUNT_KEY, CONTAINER_NAME]):
    raise ValueError("Missing Azure credentials in .env file (ACCOUNT_NAME, ACCOUNT_KEY, CONTAINER_NAME)")

# Use the ABFSS protocol for Azure Data Lake Gen2
BRONZE_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/bronze"
SILVER_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/silver"

# Keep logs locally on the execution machine
LOG_PATH = PROJECT_ROOT / "logs" / "bronze_to_silver.log"


def _get_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("bronze_to_silver")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def standardize_columns(df):
    return df.toDF(*[column.lower().strip().replace(" ", "_") for column in df.columns])


def write_silver(df, table: str, logger: logging.Logger) -> None:
    destination = f"{SILVER_BASE}/{table}"
    # We write AS Parquet to the silver layer for better performance downstream
    df.write.mode("overwrite").parquet(destination)
    logger.info("%s written to silver at %s", table, destination)


def run() -> None:
    logger = _get_logger()
    logger.info("BRONZE to SILVER pipeline started")

    spark = create_spark_session()
    
    # Configure Spark session with Azure Data Lake credentials
    spark.conf.set(
        f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net",
        ACCOUNT_KEY
    )

    try:
        # ALL READS UPDATED TO CSV WITH CORRECT AZURE FILENAMES
        
        customers = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_customers_dataset.csv", header=True, inferSchema=True)
        )
        customers = customers.dropDuplicates(["customer_id"]).fillna(
            {"customer_city": "unknown", "customer_state": "unknown"}
        )
        write_silver(customers, "customers", logger)

        orders = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_orders_dataset.csv", header=True, inferSchema=True)
        )
        orders = orders.withColumn(
            "order_purchase_timestamp", to_timestamp("order_purchase_timestamp")
        ).withColumn(
            "order_delivered_customer_date", to_timestamp("order_delivered_customer_date")
        )
        orders = orders.dropDuplicates(["order_id"])
        write_silver(orders, "orders", logger)

        items = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_order_items_dataset.csv", header=True, inferSchema=True)
        )
        items = items.withColumn("price", col("price").cast(DoubleType())).withColumn(
            "freight_value", col("freight_value").cast(DoubleType())
        )
        items = items.filter((col("price") >= 0) & (col("freight_value") >= 0))
        write_silver(items, "order_items", logger)

        payments = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_order_payments_dataset.csv", header=True, inferSchema=True)
        )
        payments = payments.withColumn("payment_value", col("payment_value").cast(DoubleType()))
        write_silver(payments, "payments", logger)

        products = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_products_dataset.csv", header=True, inferSchema=True)
        )
        products = products.select("product_id", "product_category_name")
        write_silver(products, "products", logger)

        category = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/product_category_name_translation.csv", header=True, inferSchema=True)
        )
        write_silver(category, "category_translation", logger)

        sellers = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_sellers_dataset.csv", header=True, inferSchema=True)
        )
        write_silver(sellers, "sellers", logger)

        reviews = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_order_reviews_dataset.csv", header=True, inferSchema=True)
        )
        reviews = reviews.dropDuplicates(["review_id"])
        reviews = reviews.withColumn("review_score", col("review_score").cast(IntegerType()))
        reviews = reviews.withColumn("review_creation_date", to_timestamp("review_creation_date"))
        reviews = reviews.withColumn(
            "review_answer_timestamp", to_timestamp("review_answer_timestamp")
        )
        reviews = reviews.fillna({"review_comment_title": "", "review_comment_message": ""})
        write_silver(reviews, "reviews", logger)

        geolocation = standardize_columns(
            spark.read.csv(f"{BRONZE_BASE}/olist_geolocation_dataset.csv", header=True, inferSchema=True)
        )
        geolocation = geolocation.dropDuplicates()
        write_silver(geolocation, "geolocation", logger)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise e
        
    finally:
        spark.stop()

    logger.info("BRONZE to SILVER pipeline completed")


if __name__ == "__main__":
    run()
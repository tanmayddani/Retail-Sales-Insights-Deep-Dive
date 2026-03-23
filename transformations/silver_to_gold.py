import os
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql.functions import (
    avg, coalesce, col, count, countDistinct, datediff, date_format, sum, when
)

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
SILVER_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/silver"
GOLD_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/gold"

# Keep logs locally on the execution machine
LOG_PATH = PROJECT_ROOT / "logs" / "silver_to_gold.log"


def _get_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("silver_to_gold")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def _write(df, table: str, logger: logging.Logger) -> None:
    destination = f"{GOLD_BASE}/{table}"
    df.write.mode("overwrite").parquet(destination)
    logger.info("%s written to gold at %s", table, destination)


def run() -> None:
    logger = _get_logger()
    logger.info("SILVER to GOLD pipeline started: Building Star Schema")

    spark = create_spark_session()
    
    # Configure Spark session with Azure Data Lake credentials
    spark.conf.set(
        f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net",
        ACCOUNT_KEY
    )

    try:
        # 1. Read from Silver
        orders = spark.read.parquet(f"{SILVER_BASE}/orders")
        order_items = spark.read.parquet(f"{SILVER_BASE}/order_items")
        customers = spark.read.parquet(f"{SILVER_BASE}/customers")
        payments = spark.read.parquet(f"{SILVER_BASE}/payments")
        products = spark.read.parquet(f"{SILVER_BASE}/products")
        category_translation = spark.read.parquet(f"{SILVER_BASE}/category_translation")
        reviews = spark.read.parquet(f"{SILVER_BASE}/reviews")
        sellers = spark.read.parquet(f"{SILVER_BASE}/sellers")

        # ==========================================
        # DIMENSION TABLES
        # ==========================================

        # DIM_DATE: Created dynamically from order timestamps
        dim_date = orders.select(
            date_format("order_purchase_timestamp", "yyyy-MM-dd").alias("date_key"),
            date_format("order_purchase_timestamp", "yyyy").cast("int").alias("year"),
            date_format("order_purchase_timestamp", "MM").cast("int").alias("month"),
            date_format("order_purchase_timestamp", "dd").cast("int").alias("day"),
            date_format("order_purchase_timestamp", "E").alias("day_of_week")
        ).distinct().na.drop(subset=["date_key"])
        _write(dim_date, "dim_date", logger)

        # DIM_CUSTOMERS: Includes LTV and Segmentation logic directly in the dimension
        ltv_calc = (
            order_items.join(orders, "order_id")
            .join(customers, "customer_id")
            .groupBy("customer_unique_id")
            .agg(
                countDistinct("order_id").alias("lifetime_orders"), # <-- FIX: Removed the table prefix
                sum("price").alias("lifetime_value")
            )
        )
        
        dim_customers = customers.join(ltv_calc, "customer_unique_id", "left").withColumn(
            "customer_segment",
            when(col("lifetime_value") >= 1000, "High Value")
            .when(col("lifetime_orders") >= 3, "Repeat")
            .otherwise("Emerging")
        )
        _write(dim_customers, "dim_customers", logger)

        # DIM_PRODUCTS: English translations applied
        dim_products = products.join(category_translation, "product_category_name", "left").select(
            "product_id",
            coalesce(col("product_category_name_english"), col("product_category_name")).alias("category_name")
        )
        _write(dim_products, "dim_products", logger)

        # DIM_SELLERS: Pass through from silver
        dim_sellers = sellers
        _write(dim_sellers, "dim_sellers", logger)

        # ==========================================
        # FACT TABLES
        # ==========================================

        # FACT_SALES: Granularity = Individual Order Item
        fact_sales = order_items.join(orders, "order_id", "inner").select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            date_format("order_purchase_timestamp", "yyyy-MM-dd").alias("date_key"),
            "price",
            "freight_value"
        )
        _write(fact_sales, "fact_sales", logger)

        # FACT_ORDERS: Granularity = Order Level (Aggregated Payments, Reviews, Delivery)
        payment_summary = payments.groupBy("order_id").agg(
            sum("payment_value").alias("total_payment_value")
        )
        review_summary = reviews.groupBy("order_id").agg(
            avg("review_score").alias("avg_review_score")
        )
        
        fact_orders = (
            orders.join(payment_summary, "order_id", "left")
            .join(review_summary, "order_id", "left")
            .select(
                "order_id",
                "customer_id",
                date_format("order_purchase_timestamp", "yyyy-MM-dd").alias("date_key"),
                "order_status",
                datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")).alias("delivery_days"),
                "total_payment_value",
                "avg_review_score"
            )
        )
        _write(fact_orders, "fact_orders", logger)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise e
        
    finally:
        spark.stop()

    logger.info("SILVER to GOLD Star Schema pipeline completed")


if __name__ == "__main__":
    run()
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.spark_session import create_spark_session

RAW_BASE = PROJECT_ROOT / "data" / "raw"
BRONZE_BASE = PROJECT_ROOT / "data" / "bronze"
LOG_PATH = PROJECT_ROOT / "logs" / "raw_to_bronze.log"

DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def _get_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("raw_to_bronze")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def run() -> None:
    logger = _get_logger()
    logger.info("RAW to BRONZE pipeline started")

    BRONZE_BASE.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session()

    try:
        for table, file_name in DATASETS.items():
            source_path = RAW_BASE / file_name

            if not source_path.exists():
                logger.warning("%s not found. Skipping %s.", file_name, table)
                continue

            logger.info("Reading %s", source_path)
            df = spark.read.option("header", True).csv(str(source_path))

            row_count = df.count()
            logger.info("%s row count: %s", table, row_count)

            destination = BRONZE_BASE / table
            df.write.mode("overwrite").parquet(str(destination))
            logger.info("%s written to bronze at %s", table, destination)
    finally:
        spark.stop()

    logger.info("RAW to BRONZE pipeline completed")


if __name__ == "__main__":
    run()

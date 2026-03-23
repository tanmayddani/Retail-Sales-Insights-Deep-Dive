import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, min as _min, max as _max

# Import your Azure credentials
from transformations.bronze_to_silver import ACCOUNT_NAME, ACCOUNT_KEY, CONTAINER_NAME

# Configure the logger to print to console AND write to 'gold_test.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("gold_test.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gold_test")

GOLD_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/gold"

def validate_azure_gold(spark: SparkSession, gold_base_path: str, table_name: str, metrics_to_check: list, logger: logging.Logger) -> bool:
    azure_path = f"{gold_base_path}/{table_name}"
    logger.info(f"--- AZURE GOLD LAYER CHECK: {table_name.upper()} ---")
    
    try:
        # Read the aggregated Parquet files
        df = spark.read.parquet(azure_path)
        total_count = df.count()
        
        if total_count == 0:
            logger.error(f"FAIL: {table_name} aggregated to 0 rows in Azure Gold!")
            return False
            
        logger.info(f"PASS: {table_name} is readable. Rows: {total_count}, Columns: {len(df.columns)}")
        
        # --- Null Value Counting Logic ---
        # Note: Nulls are sometimes expected in Gold due to LEFT JOINs, but we still log them.
        null_counts = df.select([
            _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
        ]).collect()[0].asDict()
        
        nulls_found = False
        for col_name, null_count in null_counts.items():
            if null_count > 0:
                nulls_found = True
                null_pct = round((null_count / total_count) * 100, 2)
                logger.warning(f"NULLS (EXPECTED FROM JOINS?): '{col_name}' has {null_count} nulls ({null_pct}%).")
                
        if not nulls_found:
            logger.info(f"PASS: No null values found in {table_name}.")

        # --- NEW: Business Metric Sanity Checks ---
        if metrics_to_check:
            for metric in metrics_to_check:
                if metric in df.columns:
                    # Get the minimum and maximum values for the numeric column
                    stats = df.select(_min(col(metric)).alias("min"), _max(col(metric)).alias("max")).collect()[0]
                    
                    logger.info(f"METRIC '{metric}': Min = {stats['min']}, Max = {stats['max']}")
                    
                    # Ensure money/time metrics are not negative
                    if stats['min'] is not None and float(stats['min']) < 0:
                        logger.error(f"BUSINESS LOGIC FAIL: Negative values detected in '{metric}'!")
                    else:
                        logger.info(f"PASS (BUSINESS LOGIC): '{metric}' has valid positive ranges.")

        return True
        
    except Exception as e:
        logger.error(f"FAIL: Could not query {table_name} from Azure Gold. Error: {str(e)}")
        return False

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    from utils.spark_session import create_spark_session
    
    logger.info("Starting manual Gold validation test for all analytical tables...")
    
    # 1. Create the Spark session
    spark = create_spark_session()
    
    # 2. Authenticate Spark to Azure using your key
    spark.conf.set(
        f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net", 
        ACCOUNT_KEY
    )
    
    # 3. Dictionary of Gold tables mapped to the specific numeric metrics we want to sanity check
    tables_to_test = {
        "sales_mart": ["price", "freight_value", "payment_value", "delivery_days"],
        "dashboard_cube": ["revenue", "payment_value", "avg_delivery_days"],
        "sales_summary": ["total_revenue", "avg_order_value"],
        "revenue_by_state": ["total_revenue"],
        "top_customers": ["lifetime_value"],
        "product_performance": ["total_revenue"],
        "monthly_sales": ["monthly_revenue"],
        "category_performance": ["total_revenue"],
        "payment_mix": ["total_payment_value"],
        "delivery_performance": ["avg_delivery_days"],
        "customer_segments": ["lifetime_value"]
    }
    
    # 4. Loop through and validate every table
    for table_name, metrics in tables_to_test.items():
        validate_azure_gold(spark, GOLD_BASE, table_name, metrics, logger)
        logger.info("-" * 50)
    
    logger.info("Test execution finished. Shutting down Spark...")
    
    # Give Windows 2 seconds to release file locks before shutting down
    time.sleep(2)
    spark.stop()
    logger.info("Spark shut down successfully.")
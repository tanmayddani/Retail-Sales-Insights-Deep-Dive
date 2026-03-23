import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

# Make sure to import ACCOUNT_KEY as well so Spark can connect to Azure
from transformations.bronze_to_silver import ACCOUNT_NAME, ACCOUNT_KEY, CONTAINER_NAME

# Configure the logger to print to console AND write to 'bronze_test.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("bronze_test.log", mode='w'), # 'w' overwrites the log each run. Use 'a' to append.
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bronze_test")

BRONZE_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/bronze"

def validate_azure_bronze(spark: SparkSession, bronze_base_path: str, file_name: str, logger: logging.Logger) -> bool:
    azure_path = f"{bronze_base_path}/{file_name}"
    logger.info(f"--- AZURE BRONZE LAYER CHECK: {file_name} ---")
    
    try:
        df = spark.read.csv(azure_path, header=True, inferSchema=True)
        total_count = df.count()
        
        if total_count == 0:
            logger.error(f"FAIL: {file_name} is completely empty in Azure!")
            return False
            
        logger.info(f"PASS: {file_name} is readable. Rows: {total_count}, Columns: {len(df.columns)}")
        
        # --- NEW: Null Value Counting Logic ---
        null_counts = df.select([
            _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
        ]).collect()[0].asDict()
        
        nulls_found = False
        for col_name, null_count in null_counts.items():
            if null_count > 0:
                nulls_found = True
                null_pct = round((null_count / total_count) * 100, 2)
                logger.warning(f"NULLS: '{col_name}' contains {null_count} nulls ({null_pct}%).")
                
        if not nulls_found:
            logger.info(f"PASS: No null values found in {file_name}.")
            
        return True
        
    except Exception as e:
        logger.error(f"FAIL: Could not read {file_name} from Azure Bronze. Error: {str(e)}")
        return False

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    from utils.spark_session import create_spark_session
    
    logger.info("Starting manual Bronze validation test for all files...")
    
    # 1. Create the Spark session
    spark = create_spark_session()
    
    # 2. Authenticate Spark to Azure using your key
    spark.conf.set(
        f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net", 
        ACCOUNT_KEY
    )
    
    # 3. List of all your raw datasets
    files_to_test = [
        "olist_customers_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "product_category_name_translation.csv"
    ]
    
    # 4. Loop through and validate every file
    for test_file in files_to_test:
        validate_azure_bronze(spark, BRONZE_BASE, test_file, logger)
        logger.info("-" * 50) # Just a visual separator in the logs
    
    logger.info("Test execution finished. Shutting down Spark...")
    
    # Give Windows 2 seconds to release file locks before shutting down
    time.sleep(2)
    spark.stop()
    logger.info("Spark shut down successfully.")
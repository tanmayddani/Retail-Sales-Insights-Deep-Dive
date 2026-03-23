import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

# Import your Azure credentials
from transformations.bronze_to_silver import ACCOUNT_NAME, ACCOUNT_KEY, CONTAINER_NAME

# Configure the logger to print to console AND write to 'silver_test.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("silver_test.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("silver_test")

SILVER_BASE = f"abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/silver"

def validate_azure_silver(spark: SparkSession, silver_base_path: str, table_name: str, primary_key: str, logger: logging.Logger) -> bool:
    azure_path = f"{silver_base_path}/{table_name}"
    logger.info(f"--- AZURE SILVER LAYER CHECK: {table_name.upper()} ---")
    
    try:
        # NOTE: Reading as Parquet instead of CSV
        df = spark.read.parquet(azure_path)
        total_count = df.count()
        
        if total_count == 0:
            logger.error(f"FAIL: {table_name} is completely empty in Azure Silver!")
            return False
            
        logger.info(f"PASS: {table_name} is readable. Rows: {total_count}, Columns: {len(df.columns)}")
        
        # --- NEW: Primary Key Uniqueness Check ---
        if primary_key:
            distinct_pk_count = df.select(primary_key).distinct().count()
            if distinct_pk_count == total_count:
                logger.info(f"PASS (TRANSFORMATION): Primary key '{primary_key}' is 100% unique.")
            else:
                duplicates = total_count - distinct_pk_count
                logger.warning(f"WARN (TRANSFORMATION): '{primary_key}' has {duplicates} duplicate records left over!")

        # --- Null Value Counting Logic ---
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
            logger.info(f"PASS: No null values found in {table_name}.")
            
        return True
        
    except Exception as e:
        logger.error(f"FAIL: Could not query {table_name} from Azure Silver. Error: {str(e)}")
        return False

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    from utils.spark_session import create_spark_session
    
    logger.info("Starting manual Silver validation test for all tables...")
    
    # 1. Create the Spark session
    spark = create_spark_session()
    
    # 2. Authenticate Spark to Azure using your key
    spark.conf.set(
        f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net", 
        ACCOUNT_KEY
    )
    
    # 3. Dictionary of Silver tables mapped to their Primary Keys
    # If a table doesn't have a single primary key (like order_items), we pass None
    tables_to_test = {
        "customers": "customer_id",
        "orders": "order_id",
        "order_items": None, 
        "payments": None,
        "products": "product_id",
        "sellers": "seller_id",
        "category_translation": "product_category_name",
        "reviews": "review_id",
        "geolocation": None
    }
    
    # 4. Loop through and validate every table
    for table_name, pk in tables_to_test.items():
        validate_azure_silver(spark, SILVER_BASE, table_name, pk, logger)
        logger.info("-" * 50)
    
    logger.info("Test execution finished. Shutting down Spark...")
    
    # Give Windows 2 seconds to release file locks before shutting down
    time.sleep(2)
    spark.stop()
    logger.info("Spark shut down successfully.")
import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),  # Saves logs to this file
        logging.StreamHandler()               # Prints logs to the console
    ]
)

# Define the logger variable (This was the missing line!)
logger = logging.getLogger(__name__)

# Mute the chatty Azure HTTP request logs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# Load environment variables from the .env file
load_dotenv()

# Read values from .env
connection_string = os.getenv("CONNECTION_STRING")
container_name = os.getenv("CONTAINER_NAME")

# Validate that the necessary variables were found
if not connection_string or not container_name:
    logger.error("Missing CONNECTION_STRING or CONTAINER_NAME in the .env file.")
    raise ValueError("Missing CONNECTION_STRING or CONTAINER_NAME in the .env file.")

# Connect to Azure Data Lake
try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    logger.info("Successfully connected to Azure Storage.")
except Exception as e:
    logger.error(f"Failed to connect to Azure: {e}")
    exit(1)

# Local dataset paths
files_to_upload = [
    "data/raw/olist_customers_dataset.csv",
    "data/raw/olist_orders_dataset.csv",
    "data/raw/olist_order_items_dataset.csv",
    "data/raw/olist_order_payments_dataset.csv",
    "data/raw/olist_order_reviews_dataset.csv",
    "data/raw/olist_products_dataset.csv",
    "data/raw/olist_sellers_dataset.csv",
    "data/raw/olist_geolocation_dataset.csv",
    "data/raw/product_category_name_translation.csv"
]

logger.info("Starting file upload process...")

for file_path in files_to_upload:
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        
        # Uploading to the 'bronze' folder within the container
        blob_path = f"bronze/{file_name}"
        
        try:
            # Create a blob client for the specific file
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            # Upload the file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            logger.info(f"Success: '{file_name}' uploaded to '{container_name}/{blob_path}'")
            
        except Exception as e:
            logger.error(f"Error uploading {file_name}: {e}")
    else:
        logger.warning(f"File not found locally at {file_path}")

logger.info("Upload process completed.")
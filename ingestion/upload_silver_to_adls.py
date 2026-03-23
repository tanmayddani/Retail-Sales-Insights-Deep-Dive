import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")
container_name = os.getenv("CONTAINER_NAME")

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("Connected to Azure Data Lake")

local_silver_path = "data/silver"

for root, dirs, files in os.walk(local_silver_path):

    for file in files:

        if file.endswith(".parquet"):

            local_file = os.path.join(root, file)

            relative_path = os.path.relpath(local_file, local_silver_path)

            blob_path = f"silver/{relative_path}"

            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            with open(local_file, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f"Uploaded: {blob_path}")

print("Silver upload completed")
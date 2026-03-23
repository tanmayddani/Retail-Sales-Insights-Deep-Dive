import os
from dotenv import load_dotenv

load_dotenv()

STORAGE_ACCOUNT = os.getenv("ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
STORAGE_KEY = os.getenv("ACCOUNT_KEY")

if not STORAGE_ACCOUNT or not CONTAINER_NAME:
    raise ValueError("Missing Azure Storage configuration in .env file")
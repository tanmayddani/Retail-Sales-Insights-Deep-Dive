import logging
import os

LOG_DIR = "logs"
LOG_FILE = "pipeline.log"

os.makedirs(LOG_DIR, exist_ok=True)

log_path = os.path.join(LOG_DIR, LOG_FILE)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def get_logger(name):
    return logging.getLogger(name)
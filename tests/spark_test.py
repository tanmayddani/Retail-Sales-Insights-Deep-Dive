import sys
import os

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.spark_session import create_spark_session


def main():

    print("Starting Spark Session...")

    spark = create_spark_session()

    data = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
    df = spark.createDataFrame(data, ["id", "name"])

    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_TEMP_DIR = PROJECT_ROOT / ".spark-temp"

def create_spark_session():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    SPARK_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Geocart_Bronze_To_Silver")
        .master("local[*]")

        # ---> THE FIX: Azure Hadoop packages added to the main builder <---
        # Change 3.3.6 to 3.3.2
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.2")

        # Windows Hadoop fixes
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")

        # Disable Hadoop commit protocol (CRITICAL)
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

        # Avoid parquet committer crash
        .config("spark.sql.parquet.output.committer.class",
                "org.apache.parquet.hadoop.ParquetOutputCommitter")

        .config("spark.local.dir", str(SPARK_TEMP_DIR))

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark
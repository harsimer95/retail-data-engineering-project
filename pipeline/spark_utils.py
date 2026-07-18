import os
import sys
from pathlib import Path


python_executable = str(Path(sys.executable).resolve())

os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Retail Data Engineering Pipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
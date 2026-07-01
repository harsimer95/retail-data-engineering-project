import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Retail Data Engineering Pipeline") \
        .getOrCreate()
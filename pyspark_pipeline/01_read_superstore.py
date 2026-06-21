import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.master("local[*]") \
.appName("Read Superstore CSV") \
.getOrCreate()

file_path = "data/raw/SampleSuperstore.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("Schema:")
df.printSchema()

print("First 5 rows:")
df.show(5)

print("Total rows:")
print(df.count())

print("Column name:")
print(df.columns)

spark.stop()
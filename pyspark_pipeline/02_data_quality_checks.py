import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Data Quality Checks") \
    .getOrCreate()

file_path = "data/raw/SampleSuperstore.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Basic dataset checks

print("Total rows:", df.count())
print("Total columns:", len(df.columns))

# Required columns check

required_columns = [
    "Row ID",
    "Order ID",
    "Order Date",
    "Ship Date",
    "Sales",
    "Quantity",
    "Discount",
    "Profit"
    ]

missing_columns = [
    column for column in required_columns
    if column not in df.columns
]

print("Missing required columns:", missing_columns)

# Missing value check

missing_values = df.select([
    spark_sum(
        when(col(column).isNull(), 1).otherwise(0)
    ).alias(column)
    for column in df.columns
])

missing_values.show()

# Duplicate row check

total_rows = df.count()
unique_rows = df.dropDuplicates().count()
duplicate_rows = total_rows - unique_rows

print("Duplicate rows:", duplicate_rows)

spark.stop()
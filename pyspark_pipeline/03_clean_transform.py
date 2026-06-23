import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, datediff

spark = SparkSession.builder \
.master("local[*]") \
.appName("Clean and Transform superstore Data") \
.getOrCreate()

file_path = "data/raw/SampleSuperstore.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Standardize column names

for column in df.columns:
    new_column = column.lower().replace(" ", "_").replace("-", "_")
    df = df.withColumnRenamed(column, new_column)

# Type conversions

df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))

df = df.withColumn("sales", col("sales").cast("double"))
df = df.withColumn("quantity", col("quantity").cast("integer"))
df = df.withColumn("discount", col("discount").cast("double"))
df = df.withColumn("profit", col("profit").cast("double"))

# Feature engineering

df = df.withColumn("order_year", year(col("order_date")))
df = df.withColumn("order_month", month(col("order_date")))
df = df.withColumn("shipping_days", datediff(col("ship_date"), col("order_date")))

# Validate transformed data

print("Transformed Schema:")
df.printSchema()

print("Sample Transformed Rows:")
df.show(5)

print("Total Rows After Transformation:")
print(df.count())

spark.stop()
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    datediff,
    when,
    lit,
    concat_ws
)

# Spark session

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Business Rules Validation") \
    .getOrCreate()

# Extract

file_path = "data/raw/SampleSuperstore.csv"

df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("encoding", "ISO-8859-1") \
    .csv(file_path)

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

# Business rule validation
# Add rejection reason for failed records

df = df.withColumn(
    "quantity_error",
    when(col("quantity").isNull(), lit("Quantity is missing"))
    .when(col("quantity") <= 0, lit("Quantity must be greater than 0"))
)

df = df.withColumn(
    "discount_error",
    when(col("discount").isNull(), lit("Discount is missing"))
    .when(
        (col("discount") < 0) | (col("discount") > 1),
        lit("Discount must be between 0 and 1")
    )
)

df = df.withColumn(
    "sales_error",
    when(col("sales").isNull(), lit("Sales is missing"))
    .when(col("sales") < 0, lit("Sales cannot be negative"))
)

df = df.withColumn(
    "shipping_date_error",
    when(col("order_date").isNull(), lit("Order date is missing"))
    .when(col("ship_date").isNull(), lit("Ship date is missing"))
    .when(
        col("ship_date") < col("order_date"),
        lit("Ship date cannot be before order date")
    )
)

df = df.withColumn(
    "rejection_reason",
    concat_ws(
        "; ",
        col("quantity_error"),
        col("discount_error"),
        col("sales_error"),
        col("shipping_date_error")
    )
)

# Split valid and rejected records

valid_df = df.filter(col("rejection_reason") == "")

rejected_df = df.filter(col("rejection_reason") != "")

valid_df = valid_df.drop(
    "quantity_error",
    "discount_error",
    "sales_error",
    "shipping_date_error"
)

rejected_df = rejected_df.drop(
    "quantity_error",
    "discount_error",
    "sales_error",
    "shipping_date_error"
)

# Validation summary

print("Transformed Schema:")
valid_df.printSchema()

print("Total records:", df.count())
print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())

print("Sample valid records:")
valid_df.show(5)

print("Sample rejected records:")
rejected_df.select(
    "row_id",
    "order_id",
    "quantity",
    "discount",
    "sales",
    "order_date",
    "ship_date",
    "rejection_reason"
).show(10, truncate=False)

spark.stop()
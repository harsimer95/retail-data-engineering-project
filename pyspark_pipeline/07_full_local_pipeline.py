import os
import sys
import shutil

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

# ====================================================
# FILE PATH SETUP
# ====================================================

incoming_file_path = "data/incoming/sales_2026-06-24.csv"

file_name = os.path.basename(incoming_file_path)
folder_name = os.path.splitext(file_name)[0]

raw_file_path = f"data/raw/{file_name}"
archive_file_path = f"data/archive/{file_name}"

processed_output_path = f"data/processed/{folder_name}"
rejected_output_path = f"data/rejected/{folder_name}"

# ====================================================
# MOVE FILE THROUGH DATA LAKE ZONES
# ====================================================

shutil.copy2(incoming_file_path, raw_file_path)
print("File copied to raw zone:", raw_file_path)

# ====================================================
# SPARK SESSION
# ====================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Full Local Retail Pipeline") \
    .getOrCreate()

# ====================================================
# EXTRACT FROM RAW ZONE
# ====================================================

df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("encoding", "ISO-8859-1") \
    .csv(raw_file_path)

# ====================================================
# STANDARDIZE COLUMN NAMES
# ====================================================

for column in df.columns:
    new_column = column.lower().replace(" ", "_").replace("-", "_")
    df = df.withColumnRenamed(column, new_column)

# ====================================================
# TYPE CONVERSIONS
# ====================================================

df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))

df = df.withColumn("sales", col("sales").cast("double"))
df = df.withColumn("quantity", col("quantity").cast("integer"))
df = df.withColumn("discount", col("discount").cast("double"))
df = df.withColumn("profit", col("profit").cast("double"))

# ====================================================
# FEATURE ENGINEERING
# ====================================================

df = df.withColumn("order_year", year(col("order_date")))
df = df.withColumn("order_month", month(col("order_date")))
df = df.withColumn("shipping_days", datediff(col("ship_date"), col("order_date")))

# ====================================================
# BUSINESS RULE VALIDATION
# ====================================================

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

# ====================================================
# SPLIT VALID AND REJECTED RECORDS
# ====================================================

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

# ====================================================
# VALIDATION SUMMARY
# ====================================================

print("Input file:", incoming_file_path)
print("Raw file:", raw_file_path)
print("Output folder name:", folder_name)

print("Total records:", df.count())
print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())

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

# ====================================================
# WRITE OUTPUT
# ====================================================

valid_df.write.mode("overwrite").parquet(processed_output_path)
rejected_df.write.mode("overwrite").parquet(rejected_output_path)

print("Valid processed data saved to:", processed_output_path)
print("Rejected data saved to:", rejected_output_path)

# ====================================================
# ARCHIVE ORIGINAL INCOMING FILE AFTER SUCCESS
# ====================================================

shutil.move(incoming_file_path, archive_file_path)
print("Original incoming file moved to archive:", archive_file_path)

spark.stop()
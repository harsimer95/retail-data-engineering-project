import os
import sys
import shutil
from datetime import datetime

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
# LOGGING SETUP
# ====================================================

LOG_FILE_PATH = "data/logs/pipeline_log.txt"


def write_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)

    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(log_message + "\n")


write_log("Pipeline started")

# ====================================================
# FILE PATH SETUP
# ====================================================

incoming_folder = "data/incoming"

incoming_files = [
    os.path.join(incoming_folder, file)
    for file in os.listdir(incoming_folder)
    if file.endswith(".csv")
]

if not incoming_files:
    write_log("No incoming CSV files found. Pipeline stopped.")
    sys.exit()

incoming_file_path = max(incoming_files, key=os.path.getmtime)

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
write_log(f"File copied to raw zone: {raw_file_path}")

# ====================================================
# SPARK SESSION
# ====================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Full Local Retail Pipeline") \
    .getOrCreate()

write_log("Spark session started")

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

write_log(f"Raw file loaded into Spark: {raw_file_path}")

# ====================================================
# STANDARDIZE COLUMN NAMES
# ====================================================

for column in df.columns:
    new_column = column.lower().replace(" ", "_").replace("-", "_")
    df = df.withColumnRenamed(column, new_column)

write_log("Column names standardized")

# ====================================================
# TYPE CONVERSIONS
# ====================================================

df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))

df = df.withColumn("sales", col("sales").cast("double"))
df = df.withColumn("quantity", col("quantity").cast("integer"))
df = df.withColumn("discount", col("discount").cast("double"))
df = df.withColumn("profit", col("profit").cast("double"))

write_log("Data type conversions completed")

# ====================================================
# FEATURE ENGINEERING
# ====================================================

df = df.withColumn("order_year", year(col("order_date")))
df = df.withColumn("order_month", month(col("order_date")))
df = df.withColumn("shipping_days", datediff(col("ship_date"), col("order_date")))

write_log("Feature engineering completed")

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

write_log("Business rule validation completed")

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

total_records = df.count()
valid_records = valid_df.count()
rejected_records = rejected_df.count()

write_log(f"Input file: {incoming_file_path}")
write_log(f"Raw file: {raw_file_path}")
write_log(f"Output folder name: {folder_name}")
write_log(f"Total records: {total_records}")
write_log(f"Valid records: {valid_records}")
write_log(f"Rejected records: {rejected_records}")

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

write_log(f"Valid processed data saved to: {processed_output_path}")
write_log(f"Rejected data saved to: {rejected_output_path}")

# ====================================================
# ARCHIVE ORIGINAL INCOMING FILE AFTER SUCCESS
# ====================================================

shutil.move(incoming_file_path, archive_file_path)
write_log(f"Original incoming file moved to archive: {archive_file_path}")

spark.stop()
write_log("Spark session stopped")
write_log("Pipeline completed successfully")
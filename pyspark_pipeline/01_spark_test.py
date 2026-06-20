import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

#Creating Spark Session
spark = SparkSession.builder \
.master("local[*]") \
.appName("Retail Data Engineering Project") \
.getOrCreate()

#Sample data
data = [
    ("Technology", 1000),
    ("Furniture", 500),
    ("Office Supplies", 300)
]

columns = ["Category", "Sales"]

#Create spark DataFrame
df = spark.createDataFrame(data, columns)

df.show()

df.printSchema()

spark.stop()
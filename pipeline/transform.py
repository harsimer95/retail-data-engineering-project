from pyspark.sql.functions import col, to_date, year, month, datediff


def standardize_column_names(df):
    for column in df.columns:
        new_column = column.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, new_column)

    return df


def convert_data_types(df):
    df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
    df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))

    df = df.withColumn("sales", col("sales").cast("double"))
    df = df.withColumn("quantity", col("quantity").cast("integer"))
    df = df.withColumn("discount", col("discount").cast("double"))
    df = df.withColumn("profit", col("profit").cast("double"))

    return df


def add_features(df):
    df = df.withColumn("order_year", year(col("order_date")))
    df = df.withColumn("order_month", month(col("order_date")))
    df = df.withColumn("shipping_days", datediff(col("ship_date"), col("order_date")))

    return df
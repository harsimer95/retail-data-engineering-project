from pyspark.sql.functions import col, when, lit, concat_ws


def apply_business_rules(df):
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

    return df


def split_valid_rejected(df):
    valid_df = df.filter(col("rejection_reason") == "")
    rejected_df = df.filter(col("rejection_reason") != "")

    columns_to_drop = [
        "quantity_error",
        "discount_error",
        "sales_error",
        "shipping_date_error"
    ]

    valid_df = valid_df.drop(*columns_to_drop)
    rejected_df = rejected_df.drop(*columns_to_drop)

    return valid_df, rejected_df
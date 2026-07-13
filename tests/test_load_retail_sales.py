from pipeline.azure_sql import (
    create_retail_sales_table,
    load_parquet_to_retail_sales
)


processed_path = "data/processed/sales_2026-07-04"

create_retail_sales_table()
load_parquet_to_retail_sales(processed_path)
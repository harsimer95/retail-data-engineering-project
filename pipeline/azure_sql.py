import os

import pandas as pd
from dotenv import load_dotenv
from mssql_python import connect


load_dotenv()

AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE")
AZURE_SQL_USERNAME = os.getenv("AZURE_SQL_USERNAME")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")


def validate_sql_configuration():
    required_values = {
        "AZURE_SQL_SERVER": AZURE_SQL_SERVER,
        "AZURE_SQL_DATABASE": AZURE_SQL_DATABASE,
        "AZURE_SQL_USERNAME": AZURE_SQL_USERNAME,
        "AZURE_SQL_PASSWORD": AZURE_SQL_PASSWORD,
    }

    missing_values = [
        name
        for name, value in required_values.items()
        if not value
    ]

    if missing_values:
        raise ValueError(
            "Missing Azure SQL environment variables: "
            + ", ".join(missing_values)
        )


def get_sql_connection():
    validate_sql_configuration()

    connection_string = (
        f"Server={AZURE_SQL_SERVER};"
        f"Database={AZURE_SQL_DATABASE};"
        f"UID={AZURE_SQL_USERNAME};"
        f"PWD={AZURE_SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return connect(connection_string)


def create_retail_sales_table():
    connection = None
    cursor = None

    create_tables_query = """
    IF OBJECT_ID('dbo.retail_sales', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.retail_sales (
            row_id INT NOT NULL,
            order_id VARCHAR(50) NOT NULL,
            order_date DATE,
            ship_date DATE,
            ship_mode VARCHAR(50),
            customer_id VARCHAR(50),
            customer_name VARCHAR(150),
            segment VARCHAR(50),
            country VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code INT,
            region VARCHAR(50),
            product_id VARCHAR(50),
            category VARCHAR(100),
            sub_category VARCHAR(100),
            product_name VARCHAR(500),
            sales DECIMAL(18, 4),
            quantity INT,
            discount DECIMAL(10, 4),
            profit DECIMAL(18, 4),
            order_year INT,
            order_month INT,
            shipping_days INT,
            rejection_reason VARCHAR(500),
            loaded_at DATETIME2 DEFAULT GETDATE(),

            CONSTRAINT PK_retail_sales
                PRIMARY KEY (order_id, row_id)
        );
    END;

    IF OBJECT_ID('dbo.retail_sales_staging', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.retail_sales_staging (
            row_id INT NOT NULL,
            order_id VARCHAR(50) NOT NULL,
            order_date DATE,
            ship_date DATE,
            ship_mode VARCHAR(50),
            customer_id VARCHAR(50),
            customer_name VARCHAR(150),
            segment VARCHAR(50),
            country VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code INT,
            region VARCHAR(50),
            product_id VARCHAR(50),
            category VARCHAR(100),
            sub_category VARCHAR(100),
            product_name VARCHAR(500),
            sales DECIMAL(18, 4),
            quantity INT,
            discount DECIMAL(10, 4),
            profit DECIMAL(18, 4),
            order_year INT,
            order_month INT,
            shipping_days INT,
            rejection_reason VARCHAR(500)
        );
    END;
    """

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        cursor.execute(create_tables_query)
        connection.commit()

        print("Table dbo.retail_sales is ready.")
        print("Table dbo.retail_sales_staging is ready.")

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def load_parquet_to_retail_sales(parquet_path):
    connection = None
    cursor = None

    staging_insert_query = """
    INSERT INTO dbo.retail_sales_staging (
        row_id,
        order_id,
        order_date,
        ship_date,
        ship_mode,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        product_id,
        category,
        sub_category,
        product_name,
        sales,
        quantity,
        discount,
        profit,
        order_year,
        order_month,
        shipping_days,
        rejection_reason
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    );
    """

    merge_query = """
    WITH deduplicated_source AS (
        SELECT
            row_id,
            order_id,
            order_date,
            ship_date,
            ship_mode,
            customer_id,
            customer_name,
            segment,
            country,
            city,
            state,
            postal_code,
            region,
            product_id,
            category,
            sub_category,
            product_name,
            sales,
            quantity,
            discount,
            profit,
            order_year,
            order_month,
            shipping_days,
            rejection_reason,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, row_id
                ORDER BY order_id, row_id
            ) AS duplicate_rank
        FROM dbo.retail_sales_staging
    )

    MERGE dbo.retail_sales AS target
    USING (
        SELECT
            row_id,
            order_id,
            order_date,
            ship_date,
            ship_mode,
            customer_id,
            customer_name,
            segment,
            country,
            city,
            state,
            postal_code,
            region,
            product_id,
            category,
            sub_category,
            product_name,
            sales,
            quantity,
            discount,
            profit,
            order_year,
            order_month,
            shipping_days,
            rejection_reason
        FROM deduplicated_source
        WHERE duplicate_rank = 1
    ) AS source

    ON (
        target.order_id = source.order_id
        AND target.row_id = source.row_id
    )

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            row_id,
            order_id,
            order_date,
            ship_date,
            ship_mode,
            customer_id,
            customer_name,
            segment,
            country,
            city,
            state,
            postal_code,
            region,
            product_id,
            category,
            sub_category,
            product_name,
            sales,
            quantity,
            discount,
            profit,
            order_year,
            order_month,
            shipping_days,
            rejection_reason
        )
        VALUES (
            source.row_id,
            source.order_id,
            source.order_date,
            source.ship_date,
            source.ship_mode,
            source.customer_id,
            source.customer_name,
            source.segment,
            source.country,
            source.city,
            source.state,
            source.postal_code,
            source.region,
            source.product_id,
            source.category,
            source.sub_category,
            source.product_name,
            source.sales,
            source.quantity,
            source.discount,
            source.profit,
            source.order_year,
            source.order_month,
            source.shipping_days,
            source.rejection_reason
        );
    """

    def clean_value(value):
        if pd.isna(value):
            return None

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        if hasattr(value, "item"):
            return value.item()

        return value

    try:
        dataframe = pd.read_parquet(parquet_path)

        records = []

        for row in dataframe.itertuples(index=False):
            records.append(
                (
                    clean_value(row.row_id),
                    clean_value(row.order_id),
                    clean_value(row.order_date),
                    clean_value(row.ship_date),
                    clean_value(row.ship_mode),
                    clean_value(row.customer_id),
                    clean_value(row.customer_name),
                    clean_value(row.segment),
                    clean_value(row.country),
                    clean_value(row.city),
                    clean_value(row.state),
                    clean_value(row.postal_code),
                    clean_value(row.region),
                    clean_value(row.product_id),
                    clean_value(row.category),
                    clean_value(row.sub_category),
                    clean_value(row.product_name),
                    clean_value(row.sales),
                    clean_value(row.quantity),
                    clean_value(row.discount),
                    clean_value(row.profit),
                    clean_value(row.order_year),
                    clean_value(row.order_month),
                    clean_value(row.shipping_days),
                    clean_value(row.rejection_reason),
                )
            )

        processed_records = len(records)

        connection = get_sql_connection()
        cursor = connection.cursor()

        # Start every run with an empty staging table
        cursor.execute("TRUNCATE TABLE dbo.retail_sales_staging")

        # Load the complete batch into staging
        if records:
            cursor.executemany(
                staging_insert_query,
                records
            )

        # Count final-table rows before MERGE
        cursor.execute("SELECT COUNT(*) FROM dbo.retail_sales")
        rows_before_merge = cursor.fetchone()[0]

        # Insert only records that do not already exist
        cursor.execute(merge_query)

        # Count final-table rows after MERGE
        cursor.execute("SELECT COUNT(*) FROM dbo.retail_sales")
        rows_after_merge = cursor.fetchone()[0]

        inserted_records = rows_after_merge - rows_before_merge
        duplicate_records = processed_records - inserted_records

        connection.commit()

        print("\nAzure SQL Load Summary")
        print("-" * 35)
        print(f"Parquet records processed : {processed_records}")
        print(f"Rows loaded to staging    : {processed_records}")
        print(f"Rows inserted             : {inserted_records}")
        print(f"Duplicate rows skipped    : {duplicate_records}")
        print(f"Total rows in Azure SQL   : {rows_after_merge}")

        return {
            "processed_records": processed_records,
            "inserted_records": inserted_records,
            "duplicate_records": duplicate_records,
            "total_database_rows": rows_after_merge,
        }

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()
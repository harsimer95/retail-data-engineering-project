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

    create_table_query = """
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
    """

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        cursor.execute(create_table_query)
        connection.commit()

        print("Table dbo.retail_sales is ready.")

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

    insert_query = """
    IF NOT EXISTS (
        SELECT 1
        FROM dbo.retail_sales
        WHERE order_id = ? AND row_id = ?
    )
    BEGIN
        INSERT INTO dbo.retail_sales (
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
    END;
    """

    def clean_value(value):
        if pd.isna(value):
            return None

        if hasattr(value, "item"):
            return value.item()

        return value

    try:
        dataframe = pd.read_parquet(parquet_path)

        connection = get_sql_connection()
        cursor = connection.cursor()

        processed_records = 0

        for row in dataframe.itertuples(index=False):
            values = [
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
            ]

            parameters = (
                values[1],
                values[0],
                *values
            )

            cursor.execute(insert_query, parameters)
            processed_records += 1

        connection.commit()

        cursor.execute("SELECT COUNT(*) FROM dbo.retail_sales")
        total_database_rows = cursor.fetchone()[0]

        print(f"Parquet records processed: {processed_records}")
        print(f"Total rows in dbo.retail_sales: {total_database_rows}")

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()
import os

from dotenv import load_dotenv
from mssql_python import connect

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

missing_values = [
    name
    for name, value in {
        "AZURE_SQL_SERVER": server,
        "AZURE_SQL_DATABASE": database,
        "AZURE_SQL_USERNAME": username,
        "AZURE_SQL_PASSWORD": password,
    }.items()
    if not value
]

if missing_values:
    raise ValueError(
        f"Missing Azure SQL environment variables: {', '.join(missing_values)}"
    )

connection_string = (
    f"Server={server};"
    f"Database={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

connection = None
cursor = None

try:
    connection = connect(connection_string)
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            DB_NAME() AS database_name,
            SUSER_SNAME() AS login_name,
            GETDATE() AS server_time
        """
    )

    row = cursor.fetchone()

    print("Azure SQL connection successful.")
    print("Database:", row[0])
    print("Login:", row[1])
    print("Server time:", row[2])

finally:
    if cursor is not None:
        cursor.close()

    if connection is not None:
        connection.close()
        print("Azure SQL connection closed.")
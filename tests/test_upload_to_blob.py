from pipeline.azure_blob import upload_file

upload_file(
    container_name="raw",
    local_file_path="data/raw/sales_2026-06-30.csv",
    blob_name="sales_2026-06-30.csv"
)
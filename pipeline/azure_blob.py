import os

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# ====================================================
# LOAD AZURE CONNECTION
# ====================================================

load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if not AZURE_CONNECTION_STRING:
    raise ValueError(
        "AZURE_STORAGE_CONNECTION_STRING not found in .env file"
    )

# ====================================================
# CREATE BLOB SERVICE CLIENT
# ====================================================

blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_CONNECTION_STRING
)

# ====================================================
# LIST BLOBS
# ====================================================

def list_blobs(container_name):
    container_client = blob_service_client.get_container_client(container_name)

    print(f"\nBlobs in container: {container_name}")

    for blob in container_client.list_blobs():
        print(blob.name)

# ====================================================
# UPLOAD SINGLE FILE
# ====================================================

def upload_file(container_name, local_file_path, blob_name):

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    with open(local_file_path, "rb") as file:
        blob_client.upload_blob(
            file,
            overwrite=True
        )

    print(
        f"Uploaded {local_file_path} "
        f"to {container_name}/{blob_name}"
    )

# ====================================================
# UPLOAD ENTIRE FOLDER
# ====================================================

def upload_folder(container_name, local_folder_path, blob_folder_name):

    for root, dirs, files in os.walk(local_folder_path):

        for file_name in files:

            # Skip Spark checksum files and marker files
            if file_name.startswith("."):
                continue

            if file_name == "_SUCCESS":
                continue

            if not file_name.endswith(".parquet"):
                continue

            local_file_path = os.path.join(root, file_name)

            relative_path = os.path.relpath(
                local_file_path,
                local_folder_path
            )

            blob_name = os.path.join(
                blob_folder_name,
                relative_path
            ).replace("\\", "/")

            upload_file(
                container_name=container_name,
                local_file_path=local_file_path,
                blob_name=blob_name
            )

# ====================================================
# DOWNLOAD FILE (FOR FUTURE USE)
# ====================================================

def download_file(container_name, blob_name, download_path):

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    with open(download_path, "wb") as file:
        file.write(
            blob_client.download_blob().readall()
        )

    print(
        f"Downloaded {blob_name} "
        f"to {download_path}"
    )
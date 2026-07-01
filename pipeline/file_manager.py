import os
import sys
import shutil

from pipeline.config import (
    INCOMING_FOLDER,
    RAW_FOLDER,
    PROCESSED_FOLDER,
    REJECTED_FOLDER,
    ARCHIVE_FOLDER
)
from pipeline.logger import write_log


def get_latest_incoming_file():
    incoming_files = [
        os.path.join(INCOMING_FOLDER, file)
        for file in os.listdir(INCOMING_FOLDER)
        if file.endswith(".csv")
    ]

    if not incoming_files:
        write_log("No incoming CSV files found. Pipeline stopped.")
        sys.exit()

    latest_file = max(incoming_files, key=os.path.getmtime)
    write_log(f"Latest incoming file detected: {latest_file}")

    return latest_file


def prepare_file_paths(incoming_file_path):
    file_name = os.path.basename(incoming_file_path)
    folder_name = os.path.splitext(file_name)[0]

    raw_file_path = os.path.join(RAW_FOLDER, file_name)
    archive_file_path = os.path.join(ARCHIVE_FOLDER, file_name)
    processed_output_path = os.path.join(PROCESSED_FOLDER, folder_name)
    rejected_output_path = os.path.join(REJECTED_FOLDER, folder_name)

    return {
        "file_name": file_name,
        "folder_name": folder_name,
        "raw_file_path": raw_file_path,
        "archive_file_path": archive_file_path,
        "processed_output_path": processed_output_path,
        "rejected_output_path": rejected_output_path
    }


def copy_to_raw(incoming_file_path, raw_file_path):
    shutil.copy2(incoming_file_path, raw_file_path)
    write_log(f"File copied to raw zone: {raw_file_path}")


def archive_incoming_file(incoming_file_path, archive_file_path):
    shutil.move(incoming_file_path, archive_file_path)
    write_log(f"Original incoming file moved to archive: {archive_file_path}")
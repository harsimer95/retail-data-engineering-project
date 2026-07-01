from datetime import datetime
from pipeline.config import LOG_FILE_PATH


def write_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"

    print(log_message)

    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(log_message + "\n")
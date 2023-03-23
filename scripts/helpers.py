import os
import shutil
import subprocess
from pathlib import Path

import requests

from dotenv import load_dotenv

load_dotenv(".env")

DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
PSQL = os.getenv("PSQL")


def round_number_to_x(number, x):
    return x * round(number / x)


def run_shp2pgsql(command):
    new_command = f"{command} | {PSQL} -U {DB_USER} -d {DB_NAME}"
    shp2pgsql_output = subprocess.run(new_command, shell=True, capture_output=True, text=True)


def download(url):
    start_message = "Started downloading"
    zip_file_path = Path(url.lstrip("https://"))
    return zip_file_path

    start_message = f"{start_message} - {zip_file_path}"
    print(start_message, end="\r")

    parent = zip_file_path.resolve().parent
    parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True)

    total = int(response.headers.get('content-length', 0))
    if total == 0:
        print("\nTotal size is not in content-length")

    start = 0
    with open(zip_file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                size = f.write(chunk)
                start += size

                if total == 0:
                    continue

                current_status = (start / total) * 100
                rounded_status = round_number_to_x(current_status, 5)
                print(f"{start_message} - {rounded_status}%", end="\r")

    print(f"\nDownload completed - size - {round(start/(1024**2),2)} MB")
    return zip_file_path


def clear_temp(temp_dir):
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    temp_dir.mkdir(parents=True, exist_ok=True)

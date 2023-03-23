import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv(".env")

DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
PSQL = os.getenv("PSQL")
YEAR = os.getenv("YEAR")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
GISDATA_FOLDER = Path(GISDATA_FOLDER)


def round_number_to_x(number, x):
    return x * round(number / x)


def run_shp2pgsql(command):
    new_command = f"{command} | {PSQL} -U {DB_USER} -d {DB_NAME}"
    shp2pgsql_output = subprocess.run(new_command, shell=True, capture_output=True, text=True)


def download(url):
    current_working_directory = os.getcwd()
    os.chdir(GISDATA_FOLDER)

    start_message = "Started downloading"
    zip_file_path = Path(url.lstrip("https://"))

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

    os.chdir(current_working_directory)
    return zip_file_path


def clear_temp(temp_dir):
    # this might create issues if current working directory is temp_dir
    # and os.getcwd() is called before adding some data like extracting files to this folder
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    temp_dir.mkdir(parents=True, exist_ok=True)


def extract_folders_of_given_section(section, country_fips, extract_to_folder):
    BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
    os.chdir(GISDATA_FOLDER / BASE_PATH / section.upper())

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{country_fips}") and z.endswith(f"_{section}.zip"):
            with zipfile.ZipFile(z) as current_file:
                current_file.extractall(extract_to_folder)

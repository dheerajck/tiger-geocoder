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
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
PGPORT = os.getenv("DB_PORT")


PSQL = os.getenv("PSQL")
YEAR = os.getenv("YEAR")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = GISDATA_FOLDER / "temp"

BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"


def round_number_to_x(number, x):
    return x * round(number / x)


def run_shp2pgsql(command, os_name=None):
    if os_name == "windows":
        password = f"set PGPASSWORD={DB_PASSWORD}"
        port = f"set PGPORT={PGPORT}"
    else:
        password = f"export PGPASSWORD={DB_PASSWORD}"
        port = f"export PGPORT={PGPORT}"

    new_command = f"""
    {password}
    {command} | {PSQL} -U {DB_USER} -d {DB_NAME} -h {DB_HOST} -p {PGPORT}
    """

    shp2pgsql_output = subprocess.run(new_command, shell=True, capture_output=True, text=True)
    print(shp2pgsql_output.stdout)
    print()
    print()
    print(shp2pgsql_output.stderr)


def download(url):
    current_working_directory = os.getcwd()
    os.chdir(GISDATA_FOLDER)

    start_message = "Started downloading"
    zip_file_path = Path(url.lstrip("https://")).resolve()

    start_message = f"{start_message} - {zip_file_path}"
    print(start_message, end="\r")

    parent = zip_file_path.parent
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

    # returns full path, in gisdata as its resolved before changing direcory
    return zip_file_path


def clear_temp(temp_dir):
    # this might create issues if current working directory is temp_dir
    # and os.getcwd() is called before adding some data like extracting files to this folder

    # Because the new directory and the old one will not be the same.
    # So if a program is sitting in the directory, waiting for things, it will have the rug pulled out from under it. –
    # https://stackoverflow.com/a/186236

    # current_working_directory = os.getcwd()
    # clear_temp(TEMP_DIR)
    # print(os.getcwd())
    # this will throw os.getcwd no such file or directory
    # to fix we need to do os.chdir(TEMP_DIR)

    # first working code which throw error no such file or directory
    # when accessing this folder again even when the same folder is created
    # try:
    #     shutil.rmtree(temp_dir)
    # except FileNotFoundError:
    #     pass
    # temp_dir.mkdir(parents=True, exist_ok=True)

    for child in temp_dir.iterdir():
        if child.is_file:
            child.unlink()
        else:
            # shouldnt execute as there will be no folders inside temp
            raise Exception("shouldnt execute as there will be no folders inside temp")
            shutil.rmtree(child)


def download_extract(section, country_code_or_fips_number):
    # country code will always be us as tiger is just for us
    current_url = f"{BASE_URL}/{section.upper()}/tl_{YEAR}_{country_code_or_fips_number}_{section}.zip"
    clear_temp(TEMP_DIR)
    downloaded_file_full_path = download(current_url)
    with zipfile.ZipFile(downloaded_file_full_path) as current_file:
        current_file.extractall(TEMP_DIR)

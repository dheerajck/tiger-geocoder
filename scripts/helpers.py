import subprocess
from pathlib import Path
import requests
import shutil


def run_shp2pgsql(command, db_user, db_name):
    new_command = f"{command} | psql -U {db_user} -d {db_name}"
    shp2pgsql_output = subprocess.run(new_command, shell=True, capture_output=True, text=True)


def download(url):
    response = requests.get(url, stream=True)
    total = int(response.headers.get('content-length', 0))
    zip_file_path = Path(url.lstrip("https://"))

    parent = zip_file_path.resolve().parent

    parent.mkdir(parents=True, exist_ok=True)
    start = 0
    with open(zip_file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                size = f.write(chunk)
                start += size
                p = (start / total) * 100
                round_5 = 5 * round(p / 5)
                print(f"{round_5}", end="\r")
    return zip_file_path


def clear_temp(temp_dir):
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    temp_dir.mkdir(parents=True, exist_ok=True)

import os
from pathlib import Path
from dotenv import load_dotenv


load_dotenv(".env")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
GISDATA_FOLDER = Path(GISDATA_FOLDER)


def create_folders():
    GISDATA_FOLDER.mkdir(parents=True, exist_ok=True)
    TEMP_DIR = GISDATA_FOLDER / "temp"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

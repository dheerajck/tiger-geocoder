import os
import zipfile
from pathlib import Path

from dotenv import load_dotenv

from geocoder import Database

from .helpers import download, run_shp2pgsql, clear_temp


load_dotenv(".env")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
PSQL = os.getenv("PSQL")

GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = Path(f"{GISDATA_FOLDER}/temp/")

YEAR = "2022"
BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"

SHP2PGSQL = "shp2pgsql"


def load_national_data_caller():
    # Download and extract state data
    cwd = os.getcwd()
    os.chdir(GISDATA_FOLDER)

    state_url = f"{BASE_URL}/STATE/tl_{YEAR}_us_state.zip"

    # zip_file_path = "www2.census.gov/geo/tiger/TIGER2022/STATE/tl_2022_us_state.zip"
    zip_file_path = download(state_url)
    clear_temp(TEMP_DIR)

    # Create schema
    db = Database()

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    os.chdir(f"{BASE_PATH}/STATE")
    # Load data into staging table
    for z in os.listdir(os.getcwd()):
        if z.startswith("tl_") and z.endswith("state.zip"):
            with zipfile.ZipFile(z) as state_zip:
                state_zip.extractall(TEMP_DIR)

    # exit()
    os.chdir(TEMP_DIR)

    db.execute(
        "CREATE TABLE IF NOT EXISTS tiger_data.state_all(CONSTRAINT pk_state_all PRIMARY KEY (statefp),CONSTRAINT uidx_state_all_stusps  UNIQUE (stusps), CONSTRAINT uidx_state_all_gid UNIQUE (gid) ) INHERITS(tiger.state); "
    )

    command = f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_us_state.dbf tiger_staging.state"
    run_shp2pgsql(command)

    db.execute("SELECT loader_load_staged_data(lower('state'), lower('state_all')); ")

    db.execute(
        "CREATE INDEX IF NOT EXISTS tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom);"
    )
    db.execute("VACUUM ANALYZE tiger_data.state_all")

    # Download and extract county data
    os.chdir(GISDATA_FOLDER)
    county_url = f"{BASE_URL}/COUNTY/tl_{YEAR}_us_county.zip"

    # zip_file_path = "www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip"
    zip_file_path = download(county_url)
    # os.chdir(os.path.join(GISDATA_FOLDER, "COUNTY"))
    os.chdir(f"{BASE_PATH}/COUNTY")

    clear_temp(TEMP_DIR)

    # Create schema
    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    # Load data into staging table
    for z in os.listdir(os.getcwd()):
        if z.startswith("tl_") and z.endswith("county.zip"):
            with zipfile.ZipFile(z) as county_zip:
                county_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tiger_data.county_all(
            CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),
            CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)
        ) INHERITS(tiger.county);
        """
    )

    # Load data from shapefile into tiger_staging.county
    # cmd = f"{SHP2PGSQL} -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_us_county.dbf tiger_staging.county"
    # with os.popen(cmd) as pipe:
    #     with connection.cursor() as cur:
    #         cur.copy_from(pipe, "tiger_staging.county")

    command = f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_us_county.dbf tiger_staging.county"
    run_shp2pgsql(command)

    # Rename column geoid to cntyidfp and load staged data

    db.execute(
        """
        ALTER TABLE tiger_staging.county RENAME geoid TO cntyidfp;
        SELECT loader_load_staged_data(lower('county'), lower('county_all'));
        """
    )

    db.execute(
        """
        CREATE INDEX IF NOT EXISTS tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);
        CREATE UNIQUE INDEX IF NOT EXISTS uidx_tiger_data_county_all_statefp_countyfp ON tiger_data.county_all USING btree(statefp,countyfp);
    """
    )

    # Create table tiger_data.county_all_lookup

    db.execute(
        "CREATE TABLE tiger_data.county_all_lookup ( CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)) INHERITS (tiger.county_lookup);"
    )

    # Vacuum analyze tables

    db.execute("VACUUM ANALYZE tiger_data.county_all;")
    db.execute("VACUUM ANALYZE tiger_data.county_all_lookup;")

    # Insert data into tiger_data.county_all_lookup

    db.execute(
        """
        INSERT INTO tiger_data.county_all_lookup (st_code, state, co_code, name)
        SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name
        FROM tiger_data.county_all AS c
        INNER JOIN state_lookup AS s ON s.statefp = c.statefp;
    """
    )

    # Commit changes and close the connection

    os.chdir(cwd)

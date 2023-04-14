import os
from pathlib import Path

from dotenv import load_dotenv
from geocoder import Database

from .common_sql import reset_schema
from .helpers import download_extract, run_shp2pgsql


load_dotenv(".env")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
YEAR = 2022
SHP2PGSQL = os.getenv("SHP2PGSQL")

# Define paths
GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = GISDATA_FOLDER / "temp"

BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"


def load_national_data():
    db = Database()

    # Everything inside this function require temp dir as working directory
    # All functions thats called from this function that needs to change working directory change it at start and change back at end
    os.chdir(TEMP_DIR)

    ###############
    # State
    ###############

    section = "state"
    country = "us"

    start_message = f"Started to setup {section}"
    print(start_message)
    download_extract(section, country)

    reset_schema(db)

    db.execute(
        """CREATE TABLE IF NOT EXISTS tiger_data.state_all(
            CONSTRAINT pk_state_all PRIMARY KEY (statefp),
            CONSTRAINT uidx_state_all_stusps  UNIQUE (stusps), 
            CONSTRAINT uidx_state_all_gid UNIQUE (gid)
        ) INHERITS(tiger.state)
        """
    )

    # os.chdir(TEMP_DIR)
    # here tl_{YEAR}_us_state.dbf is in TEMP_DIR
    command = f"{SHP2PGSQL} -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_us_state.dbf tiger_staging.state"
    run_shp2pgsql(command)

    db.execute("SELECT loader_load_staged_data(lower('state'), lower('state_all'))")

    db.execute("CREATE INDEX IF NOT EXISTS tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom)")

    db.execute("VACUUM ANALYZE tiger_data.state_all")

    print(f"{start_message} - Done\n")

    ###############
    # County
    ###############

    section = "county"
    country = "us"

    start_message = f"Started to setup {section}"
    print(start_message)
    download_extract(section, country)

    reset_schema(db)

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tiger_data.county_all(
            CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),
            CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)
        ) INHERITS(tiger.county)
        """
    )

    command = f"{SHP2PGSQL} -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_us_county.dbf tiger_staging.county"
    run_shp2pgsql(command)

    db.execute("ALTER TABLE tiger_staging.county RENAME geoid TO cntyidfp")

    db.execute("SELECT loader_load_staged_data(lower('county'), lower('county_all'))")

    create_index_sql_queries = """
    CREATE INDEX IF NOT EXISTS tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);
    CREATE UNIQUE INDEX IF NOT EXISTS uidx_tiger_data_county_all_statefp_countyfp ON tiger_data.county_all USING btree(statefp,countyfp);
    """

    db.execute(create_index_sql_queries)

    db.execute("VACUUM ANALYZE tiger_data.county_all")

    # Create table tiger_data.county_all_lookup
    db.execute(
        """
        CREATE TABLE tiger_data.county_all_lookup(
            CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)
        ) INHERITS (tiger.county_lookup)
        """
    )

    # Insert data into tiger_data.county_all_lookup
    db.execute(
        """
        INSERT INTO tiger_data.county_all_lookup (st_code, state, co_code, name)
        SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name
        FROM tiger_data.county_all AS c
        INNER JOIN state_lookup AS s ON s.statefp = c.statefp
        """
    )

    # Vacuum analyze tables
    db.execute("VACUUM ANALYZE tiger_data.county_all_lookup")

    print(f"{start_message} - Done\n")


def load_national_data_caller():
    current_working_directory = os.getcwd()
    print("\nAdding US national data")

    print("-------------------------------------------------")
    print()
    load_national_data()
    print("-------------------------------------------------")

    os.chdir(current_working_directory)

import json
import os
import platform
import subprocess
import sys
from pathlib import Path
import time

import psycopg
from dotenv import load_dotenv
from geocoder import Database


load_dotenv(".env")

SHELL_SCRIPT_FOLDER_DUMP = Path("tiger_setup_scripts_folder")
SHELL_SCRIPT_FOLDER_DUMP.mkdir(parents=True, exist_ok=True)

ENV_DICT = {
    "UNZIPTOOL": os.getenv("UNZIPTOOL").strip(),
    "WGETTOOL": os.getenv("WGETTOOL").strip(),
    "PGPORT": os.getenv("DB_PORT").strip(),
    "PGHOST": os.getenv("DB_HOST").strip(),
    "PGUSER": os.getenv("DB_USER").strip(),
    "PGPASSWORD": os.getenv("DB_PASSWORD"),
    "PGDATABASE": os.getenv("DB_NAME"),
    "PSQL": os.getenv("PSQL").strip(),
    "GISDATA_FOLDER": os.getenv("GISDATA_FOLDER").strip(),
    "GEOCODER_STATES": os.getenv("GEOCODER_STATES").strip(),
    "YEAR": os.getenv("YEAR").strip(),
}


def create_extension(db):
    sql_command = """
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
    CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
    CREATE EXTENSION IF NOT EXISTS address_standardizer;
    CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;
    """

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command)

    except psycopg.Error as e:
        print(e)
        return None


def create_profile(db, profile_name, operating_system):
    sql_command = """
    INSERT INTO tiger.loader_platform(os, declare_sect, pgbin, wget, unzip_command, psql, path_sep,
            loader, environ_set_command, county_process_command)
    SELECT %s, declare_sect, pgbin, wget, unzip_command, psql, path_sep,
        loader, environ_set_command, county_process_command
    FROM tiger.loader_platform
    WHERE os = %s;"""

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command, (profile_name, operating_system))

    except psycopg.errors.UniqueViolation:
        pass
    except psycopg.Error as e:
        print(e)
        return None


def update_tiger_data_download_folder_path(db, folder_path, year):
    folder_path = str(Path(folder_path).resolve())
    website_root = f"https://www2.census.gov/geo/tiger/TIGER{year}"

    sql_command = "UPDATE tiger.loader_variables SET staging_fold=%s, tiger_year=%s, website_root=%s;"

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command, (folder_path, year, website_root))

    except psycopg.Error as e:
        print(e)
        return None


def update_env_variables(db, profile_name, env_dict):
    select_sql_command = "SELECT declare_sect FROM tiger.loader_platform WHERE os=%s;"

    try:
        cursor = db.connection.cursor()
        cursor.execute(select_sql_command, (profile_name,))

    except psycopg.Error as e:
        print(e)
        return None

    else:
        path_string = cursor.fetchone()[0]
        path_string_list = path_string.split("\n")

        for name, value in env_dict.items():
            if value not in [None, ""]:
                for index, path_element in enumerate(list(path_string_list)):
                    if name in path_element:
                        temp = path_element.split("=")
                        temp[-1] = value
                        path_string_list[index] = "=".join(temp)

        path_string = "\n".join(path_string_list)
        # print(path_string)

    update_sql_command = "UPDATE tiger.loader_platform SET declare_sect=%s WHERE os=%s;"

    try:
        cursor = db.connection.cursor()
        cursor.execute(
            update_sql_command,
            (path_string, profile_name),
        )

    except psycopg.Error as e:
        print(e)
        return None


def write_nation_script(db, profile_name, os_name=None):
    sql_command = "SELECT Loader_Generate_Nation_Script(%s);"

    if os_name == "windows":
        file_name = "load_nation.bat"
    else:
        file_name = "load_nation.sh"

    file_name = SHELL_SCRIPT_FOLDER_DUMP / file_name

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command, (profile_name,))
        result = cursor.fetchone()

    except psycopg.Error as e:
        raise Exception(f"write_nation_script\n{e}")

    else:
        nation_script = result[0]
        with open(file_name, "w") as f:
            f.write(nation_script)

        return nation_script


def write_state_script(db, profile_name, list_of_states, os_name=None):
    sql_command = f"SELECT Loader_Generate_Script(ARRAY{list_of_states}, %s);"

    if os_name == "windows":
        file_name = f"load_nation_{list_of_states[0]}.bat"
    else:
        file_name = f"load_nation_{list_of_states[0]}.sh"

    file_name = SHELL_SCRIPT_FOLDER_DUMP / file_name

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command, (profile_name,))
        result = cursor.fetchone()

    except psycopg.Error as e:
        raise Exception(f"write_state_script\n{e}")

    else:
        state_script_for_given_states = result[0]

        with open(file_name, "w") as f:
            f.write(state_script_for_given_states)

        return state_script_for_given_states


def create_index_and_clean_tiger_table(db):
    create_index_sql_command = "SELECT install_missing_indexes();"

    try:
        cursor = db.connection.cursor()
        cursor.execute(create_index_sql_command)

    except psycopg.Error as e:
        print(e)

    clean_tiger_sql_command = """
    vacuum (analyze, verbose) tiger.addr;
    vacuum (analyze, verbose) tiger.edges;
    vacuum (analyze, verbose) tiger.faces;
    vacuum (analyze, verbose) tiger.featnames;
    vacuum (analyze, verbose) tiger.place;
    vacuum (analyze, verbose) tiger.cousub;
    vacuum (analyze, verbose) tiger.county;
    vacuum (analyze, verbose) tiger.state;
    vacuum (analyze, verbose) tiger.zip_lookup_base;
    vacuum (analyze, verbose) tiger.zip_state;
    vacuum (analyze, verbose) tiger.zip_state_loc;"""

    for individual_command in clean_tiger_sql_command.split(";"):
        try:
            cursor.execute(individual_command)

        except psycopg.Error as e:
            print(e)


def create_folders():
    folder_path = Path(ENV_DICT["GISDATA_FOLDER"])
    folder_path.mkdir(parents=True, exist_ok=True)
    temp_folder_path = folder_path / "temp"
    temp_folder_path.mkdir(parents=True, exist_ok=True)


def is_windows_operating_system():
    if platform.system() == "Windows":
        return True
    else:
        return False


def run_script(string):
    process = subprocess.Popen(string, shell=True, stdout=subprocess.PIPE)
    for c in iter(lambda: process.stdout.read(1), b""):
        sys.stdout.buffer.write(c)

    # result = subprocess.run(string, shell=True, capture_output=True, text=True)
    # print(result.stdout)


if __name__ == "__main__":
    if ENV_DICT["GISDATA_FOLDER"] in [None, ""]:
        print("Folder name for gisdata folder is required to start setting up database")
        exit()

    available_states = list(json.load(open('abbr - fips.json')).keys())
    list_of_states_string = ENV_DICT["GEOCODER_STATES"]

    if list_of_states_string == "*":
        list_of_states = available_states
    else:
        list_of_states = list_of_states_string.split(",")
        list_of_states = [i.strip() for i in list_of_states]  # "MA, RI" wil not add RI as list_of_states = ["MA", " RI"] wouldnt load "RI"

        invalid_state_abbreviations = set(list_of_states) - set(available_states)

        if len(invalid_state_abbreviations) != 0:
            for state in invalid_state_abbreviations:
                print(f"State {state} is not a recognized US state abbreviation\n")
            exit()

    db = Database()
    create_extension(db)
    create_folders()

    if is_windows_operating_system():
        os_name = "windows"
    else:
        os_name = "sh"

    profile_name = "new"
    create_profile(db, profile_name, os_name)

    update_env_variables(db, profile_name, ENV_DICT)
    update_tiger_data_download_folder_path(db, ENV_DICT["GISDATA_FOLDER"], ENV_DICT["YEAR"])

    nation_output = write_nation_script(db, profile_name, os_name)
    run_script(nation_output)

    for state in list_of_states:
        state_output = write_state_script(db, profile_name, [state], os_name)
        run_script(state_output)
        create_index_and_clean_tiger_table(db)
        time.sleep(2)

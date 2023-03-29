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


PATH_DICT = {
    "UNZIPTOOL": os.getenv("UNZIPTOOL"),
    "WGETTOOL": os.getenv("WGETTOOL"),
    "PGPORT": os.getenv("DB_PORT"),
    "PGHOST": os.getenv("DB_HOST"),
    "PGUSER": os.getenv("DB_USER"),
    "PGPASSWORD": os.getenv("DB_PASSWORD"),
    "PGDATABASE": os.getenv("DB_NAME"),
    "PSQL": os.getenv("PSQL"),
    "GISDATA_FOLDER": os.getenv("GISDATA_FOLDER"),
    "GEOCODER_STATES": os.getenv("GEOCODER_STATES"),
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

    except psycopg.Error as e:
        print(e)
        return None


def update_folder(db, folder_path):
    folder_path = str(Path(folder_path).resolve())
    sql_command = "UPDATE tiger.loader_variables SET staging_fold=%s;"

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command, (folder_path,))

    except psycopg.Error as e:
        print(e)
        return None


def update_path(db, profile_name, path_dict):
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
        # print(path_string)

        for name, value in path_dict.items():
            if value not in [None, ""]:
                for index, path_element in enumerate(list(path_string_list)):
                    if name in path_element:
                        temp = path_element.split("=")
                        temp[-1] = value
                        path_string_list[index] = "=".join(temp)

        path_string = "\n".join(path_string_list)
        # print(path_string)

    update_sql_command = "UPDATE tiger.loader_platform SET declare_sect=%s WHERE os=%s"

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
    sql_command = "SELECT Loader_Generate_Nation_Script(%s)"

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
    sql_command = f"SELECT Loader_Generate_Script(ARRAY{list_of_states}, %s)"

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
    sql_command = """
    SELECT install_missing_indexes();
    """

    try:
        cursor = db.connection.cursor()
        cursor.execute(sql_command)

    except psycopg.Error as e:
        print(e)
        return None

    sql_command = """
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

    for sql in sql_command.split(";"):
        try:
            cursor.execute(sql)

        except psycopg.Error as e:
            print(e)
            return None


def create_folders():
    folder_path = Path(PATH_DICT["GISDATA_FOLDER"])
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
    db = Database()
    if PATH_DICT["GISDATA_FOLDER"] in [None, ""]:
        print("Folder name for gisdata folder is required to start setting up database")
        exit()

    # list_of_states = ['MA']
    list_of_states_string = PATH_DICT["GEOCODER_STATES"]

    if list_of_states_string == "*":
        list_of_states = list(json.load(open('abbr - fips.json')).keys())
    else:
        list_of_states = list_of_states_string.split(",")

    list_of_states = [i.strip() for i in list_of_states]  # "MA, RI" wil not add RI as list_of_states = ["MA", " RI"] wouldnt load "RI"

    create_extension(db)
    create_folders()

    if is_windows_operating_system():
        os_name = "windows"
    else:
        os_name = "sh"

    profile_name = "new"

    create_profile(db, profile_name, os_name)
    update_path(db, profile_name, PATH_DICT)
    update_folder(db, PATH_DICT["GISDATA_FOLDER"])

    output = write_nation_script(db, profile_name, os_name)
    run_script(output)

    for state in list_of_states:
        output = write_state_script(db, profile_name, [state], os_name)
        run_script(output)
        time.sleep(2)

    create_index_and_clean_tiger_table(db)

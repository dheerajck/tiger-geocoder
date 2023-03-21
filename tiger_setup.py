import os
import subprocess
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from geocoder import Database


load_dotenv(".env")

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
    cursor = db.connection.cursor()

    sql_command = """
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
    CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
    CREATE EXTENSION IF NOT EXISTS address_standardizer;
    CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;
    """
    # sql_command = """CREATE DATABASE project_name""" psycopg.errors.DuplicateDatabase
    try:
        cursor.execute(sql_command)
        print(cursor.fetchone())

    except psycopg.Error as e:
        print(e)
        return None


def create_folders():
    folder_path = Path(PATH_DICT["GISDATA_FOLDER"])
    folder_path.mkdir(parents=True, exist_ok=True)
    temp_folder_path = folder_path / "temp"
    temp_folder_path.mkdir(parents=True, exist_ok=True)


def is_windows_operating_system():
    import platform

    if platform.system() == "Windows":
        return True
    else:
        return False


def create_profile(db, profile_name, operating_system):
    sql_command = """
    INSERT INTO tiger.loader_platform(os, declare_sect, pgbin, wget, unzip_command, psql, path_sep,
            loader, environ_set_command, county_process_command)
    SELECT %s, declare_sect, pgbin, wget, unzip_command, psql, path_sep,
        loader, environ_set_command, county_process_command
    FROM tiger.loader_platform
    WHERE os = %s;"""
    cursor = db.connection.cursor()
    try:
        cursor.execute(sql_command, (profile_name, operating_system))
        # print(cursor.fetchone())

    except psycopg.Error as e:
        print(e)
        return None


def update_folder(db, folder_name):
    if folder_name[-1] in ["/", "\\"]:
        folder_name = folder_name[:-1]
    cursor = db.connection.cursor()
    print(folder_name)
    sql_command = "UPDATE tiger.loader_variables SET staging_fold=%s;"
    try:
        cursor.execute(sql_command, (folder_name,))
        # print(cursor.fetchone())

    except psycopg.Error as e:
        print(e)
        return None


def update_path(db, profile_name, path_dict):
    cursor = db.connection.cursor()
    sql_command = "SELECT declare_sect FROM tiger.loader_platform WHERE os=%s;"
    try:
        cursor.execute(sql_command, (profile_name,))
        # print(cursor.fetchone())

    except psycopg.Error as e:
        print(e)
        return None
    else:
        path_string = cursor.fetchone()[0]
        path_string_list = path_string.split("\n")
        print(path_string)

        for name, value in path_dict.items():
            if value not in [None, ""]:
                for index, path_element in enumerate(list(path_string_list)):
                    if name in path_element:
                        temp = path_element.split("=")
                        temp[-1] = value
                        path_string_list[index] = "=".join(temp)

        path_string = "\n".join(path_string_list)
        print(path_string)

    sql_command = "UPDATE tiger.loader_platform SET declare_sect=%s WHERE os=%s"
    try:
        cursor.execute(
            sql_command,
            (path_string, profile_name),
        )
        # print(cursor.fetchone())
    except psycopg.Error as e:
        print(e)
        # return None
        return f"echo {e}"


def write_nation_script(db, profile_name):
    cursor = db.connection.cursor()
    sql_command = "SELECT Loader_Generate_Nation_Script(%s)"
    try:
        cursor.execute(sql_command, (profile_name,))
        result = cursor.fetchone()
        # print(result)

    except psycopg.Error as e:
        print(e)
        return None
    else:
        with open("load_nation.sh", "w") as f:
            f.write(result[0])
            return result[0]


def write_state_script(db, profile_name, list_of_states):
    cursor = db.connection.cursor()

    sql_command = f"SELECT Loader_Generate_Script(ARRAY{list_of_states}, %s)"

    try:
        cursor.execute(sql_command, (profile_name,))
        result = cursor.fetchone()
        # print(result)

    except psycopg.Error as e:
        print(e)
        # return None
        return f"echo {e}"

    else:
        with open("load_states.sh", "w") as f:
            f.write(result[0])
            return result[0]


def run_script(string):
    process = subprocess.Popen(string, shell=True, stdout=subprocess.PIPE)
    for c in iter(lambda: process.stdout.read(1), b""):
        sys.stdout.buffer.write(c)

    # result = subprocess.run(string, shell=True, capture_output=True, text=True)
    # print(result.stdout)


def create_index_and_clean_tiger_table(db):
    cursor = db.connection.cursor()
    sql_command = """
    SELECT install_missing_indexes();
    """
    try:
        cursor.execute(sql_command)
        # print(cursor.fetchone())

    except psycopg.Error as e:
        print(e)
        return None

    cursor.close()
    cursor = db.connection.cursor()
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
            # print(cursor.fetchone())

        except psycopg.Error as e:
            print(e)
            return None


if __name__ == "__main__":
    db = Database()
    if PATH_DICT["GISDATA_FOLDER"] in [None, ""]:
        print("Folder name for gisdata folder is required to start setting up database")
        exit()

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

    output = write_nation_script(db, profile_name)
    run_script(output)
    # list_of_states = ['MA']
    list_of_states = PATH_DICT["GEOCODER_STATES"].split(",")
    output = write_state_script(db, profile_name, list_of_states)
    run_script(output)
    create_index_and_clean_tiger_table(db)

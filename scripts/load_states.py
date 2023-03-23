import json
import os
import re
from pathlib import Path

import requests
from dotenv import load_dotenv
from geocoder import Database

from .helpers import clear_temp, download, extract_folders_of_given_section
from .common_sql import create_state_section_table_and_add_data


load_dotenv(".env")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
GEOCODER_STATES = os.getenv("GEOCODER_STATES")
YEAR = os.getenv("YEAR")
SHP2PGSQL = os.getenv("SHP2PGSQL")


# Define paths
GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = GISDATA_FOLDER / "temp"

BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"

ABBR_FIPS = json.load(open('scripts/abbr - fips.json'))

# match substrings that starts with "tl and ends with > in a string
REGEX_tl_filename_pattern = re.compile('(?=\"tl)(.*?)(?<=>)')
# match  \ " > in a string
# find_slash_double_quote_greater_than_pattern
REGEX_specific_character_pattern = re.compile('[\">]')


def get_fips_from_abbr(abbr):
    state_to_fips = ABBR_FIPS
    return state_to_fips.get(abbr.strip(), 0)


def get_fips_files(url, fips):
    """
    Helps to download the content from the specified URL using wget, and then uses Perl and sed commands to extract the file names from the HTML content
    Specifically, the first Perl command extracts all substrings that are preceded by "tl and followed by >, and the second Perl command extracts the same substrings again
    The sed command removes the "> characters from the extracted substrings
    The resulting file names are stored as an array in the files variable
    """

    # import urllib.request
    # response = urllib.request.urlopen(url)
    # content = response.read().decode('utf-8')

    response = requests.get(url)
    content = response.text

    # temp = url.replace("/", "")
    # # with open(temp, "w") as f:
    # #     f.write(content)
    # with open(temp, "r") as f:
    #     content = f.read()

    files = REGEX_tl_filename_pattern.findall(content)

    files = [REGEX_specific_character_pattern.sub('', file) for file in files]
    matched = [file for file in files if f"tl_{YEAR}_{fips}" in file]

    return matched


def download_extract(section, fips):
    current_working_directory = os.getcwd()

    current_url = f"{BASE_URL}/{section.upper()}/tl_{YEAR}_{fips}_{section}.zip"

    download(current_url)
    clear_temp(TEMP_DIR)
    extract_folders_of_given_section(section, fips, TEMP_DIR)

    os.chdir(current_working_directory)


def download_extract_urls_of_all_files(section, fips):
    current_working_directory = os.getcwd()

    current_url = f"{BASE_URL}/{section.upper()}"
    files = get_fips_files(current_url, fips)

    for i in files:
        # files are downloaded to f"{BASE_PATH}/{section.upper()}/{i}"
        # that is same as f"{BASE_URL}/{section.upper()}/{i}".lstrip("https://")
        current_file_url = f"{BASE_URL}/{section.upper()}/{i}"
        download(current_file_url)

    clear_temp(TEMP_DIR)
    extract_folders_of_given_section(section, fips, TEMP_DIR)

    os.chdir(current_working_directory)


def load_state_data(abbr, fips):
    db = Database()
    abbr = abbr.lower()

    # Everything inside this function require temp dir as working directory
    # All functions thats called from this function that needs to change working directory change it at start and change back at end
    os.chdir(TEMP_DIR)

    ###############
    # Place
    ###############

    section = "place"
    primary_key = "plcidfp"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract(section, fips)
    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR)

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_{abbr}_place_soundex_name ON tiger_data.{abbr}_place USING btree (soundex(name))")

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_place_the_geom_gist ON tiger_data.{abbr}_place USING gist(the_geom)")

    print(f"{start_message} - Done\n")

    #############
    # Cousub
    #############

    section = "cousub"
    primary_key = "cosbidfp"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract(section, fips)
    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR)

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_cousub_the_geom_gist ON tiger_data.{abbr}_cousub USING gist(the_geom)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_cousub_countyfp ON tiger_data.{abbr}_cousub USING btree(countyfp)")

    print(f"{start_message} - Done\n")

    #############
    # Tract
    #############

    section = "tract"
    primary_key = "tract_id"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract(section, fips)
    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR)

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tract_the_geom_gist ON tiger_data.{abbr}_tract USING gist(the_geom)")

    print(f"{start_message} - Done\n")

    #############
    # Faces
    #############

    section = "faces"
    primary_key = "gid"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract_urls_of_all_files(section, fips)

    dbf_files = []
    # finding all files with specific type of name in temp folder
    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "faces" in z:
            dbf_files.append(z)

    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_faces_the_geom_gist ON tiger_data.{abbr}_faces USING gist(the_geom)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_tfid ON tiger_data.{abbr}_faces USING btree (tfid)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_countyfp ON tiger_data.{abbr}_faces USING btree (countyfp)")

    print(f"{start_message} - Done\n")

    #############
    # FeatNames
    #############

    section = "featnames"
    primary_key = "gid"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract_urls_of_all_files(section, fips)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "featnames" in z:
            dbf_files.append(z)

    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_snd_name ON tiger_data.{abbr}_featnames USING btree (soundex(name))")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_lname ON tiger_data.{abbr}_featnames USING btree (lower(name))")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_tlid_statefp ON tiger_data.{abbr}_featnames USING btree (tlid,statefp)")

    print(f"{start_message} - Done\n")

    #############
    # Edges
    #############

    section = "edges"
    primary_key = "gid"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract_urls_of_all_files(section, fips)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "edges" in z:
            dbf_files.append(z)

    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_tlid ON tiger_data.{abbr}_edges USING btree (tlid)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edgestfidr ON tiger_data.{abbr}_edges USING btree (tfidr)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_tfidl ON tiger_data.{abbr}_edges USING btree (tfidl)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_countyfp ON tiger_data.{abbr}_edges USING btree (countyfp)")

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_edges_the_geom_gist ON tiger_data.{abbr}_edges USING gist(the_geom)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_zipl ON tiger_data.{abbr}_edges USING btree (zipl)")

    print(f"{start_message} - Done\n")

    #############
    # Addr
    #############

    section = "addr"
    primary_key = "gid"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract_urls_of_all_files(section, fips)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "addr" in z:
            dbf_files.append(z)

    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_least_address ON tiger_data.{abbr}_addr USING btree (least_hn(fromhn,tohn))")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_tlid_statefp ON tiger_data.{abbr}_addr USING btree (tlid, statefp)")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_zip ON tiger_data.{abbr}_addr USING btree (zip)")

    print(f"{start_message} - Done\n")

    # #############
    # # Tabblock
    # #############

    # url = f"{BASE_URL}/TABBLOCK/tl_{YEAR}_{fips}_tabblock10.zip"
    # url = f"{BASE_URL}/TABBLOCK20/tl_{YEAR}_{fips}_tabblock20.zip"

    section = "tabblock20"
    primary_key = "geoid"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract(section, fips)
    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR)

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tabblock20_the_geom_gist ON tiger_data.{abbr}_tabblock20 USING gist(the_geom)")

    print(f"{start_message} - Done\n")

    #############
    # Block Group
    #############

    section = "bg"
    primary_key = "bg_id"

    start_message = f"Started to setup {section}"
    print(start_message)

    download_extract(section, fips)
    create_state_section_table_and_add_data(section, abbr, fips, primary_key, YEAR)

    db.execute(f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_bg_the_geom_gist ON tiger_data.{abbr}_bg USING gist(the_geom)")

    print(f"{start_message} - Done\n")


def load_zip_tables_data(abbr, fips):
    db = Database()
    abbr = abbr.lower()

    ########################
    # Zip state loc Edges
    ########################

    start_message = "Started to setup zip state loc edges"
    print(start_message)

    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_state_loc(
            CONSTRAINT pk_{abbr}_zip_state_loc PRIMARY KEY(zip, stusps, place)
        ) INHERITS (tiger.zip_state_loc)
        """
    )

    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}')")

    db.execute(
        f"""
        INSERT INTO tiger_data.{abbr}_zip_state_loc(zip,stusps,statefp,place) 
        SELECT DISTINCT e.zipl, '{abbr}', '{fips}', p.name FROM tiger_data.{abbr}_edges AS e 
        INNER JOIN tiger_data.{abbr}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) 
        INNER JOIN tiger_data.{abbr}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL
        """
    )

    db.execute(f"vacuum analyze tiger_data.{abbr}_zip_state_loc")

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_zip_state_loc_place ON tiger_data.{abbr}_zip_state_loc USING btree(soundex(place))")

    print(f"{start_message} - Done\n")

    ########################
    # Zip lookup base Edges
    ########################

    start_message = "Started to setup zip lookup base edges"
    print(start_message)

    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_lookup_base(
            CONSTRAINT pk_{abbr}_zip_state_loc_city PRIMARY KEY(zip, state, county, city, statefp)
        ) INHERITS(tiger.zip_lookup_base)
        """
    )

    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}')")

    db.execute(
        f"""
        INSERT INTO tiger_data.{abbr}_zip_lookup_base(zip,state,county,city, statefp) 
        SELECT DISTINCT e.zipl, '{abbr}', c.name,p.name,'{fips}'  FROM tiger_data.{abbr}_edges AS e 
        INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '{fips}') 
        INNER JOIN tiger_data.{abbr}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) 
        INNER JOIN tiger_data.{abbr}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL
        """
    )

    db.execute(f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_zip_lookup_base_citysnd ON tiger_data.{abbr}_zip_lookup_base USING btree(soundex(city))")

    print(f"{start_message} - Done\n")

    ########################
    # Zip state Addr
    ########################

    start_message = "Started to setup zip state addr"
    print(start_message)

    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_state(
            CONSTRAINT pk_{abbr}_zip_state PRIMARY KEY(zip, stusps)
            ) INHERITS(tiger.zip_state)
        """
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}')")

    db.execute(
        f"""
        INSERT INTO tiger_data.{abbr}_zip_state(zip,stusps,statefp) 
        SELECT DISTINCT zip, '{abbr}', '{fips}' FROM tiger_data.{abbr}_addr WHERE zip is not null
        """
    )

    print(f"{start_message} - Done\n")


def load_states_data_caller():
    current_working_directory = os.getcwd()

    if GEOCODER_STATES == "*":
        print("\n'*' detected for STATES parameter. Adding data for all US states...")
        GEOCODER_STATES_LIST = list(ABBR_FIPS.keys())
    else:
        GEOCODER_STATES_LIST = GEOCODER_STATES.split(",")

    print(f"\nAdding US states data for {GEOCODER_STATES_LIST}")

    print("-------------------------------------------------")
    print()

    for state in GEOCODER_STATES_LIST:
        abbr = state
        fips = get_fips_from_abbr(abbr)

        if fips == 0:
            print(f"Error: {abbr} is not a recognized US state abbreviation\n")
        else:
            print(f"Loading state data for: {abbr} {fips}\n")
            load_state_data(abbr, fips)
            load_zip_tables_data(abbr, fips)

    print("-------------------------------------------------")

    os.chdir(current_working_directory)

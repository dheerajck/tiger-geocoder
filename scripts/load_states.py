import json
import os
import re
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from geocoder import Database

from .helpers import clear_temp, download
from .load_states_common_sql import common_sql

load_dotenv(".env")

DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
GEOCODER_STATES = os.getenv("GEOCODER_STATES")
YEAR = os.getenv("YEAR")

# PSQL = os.getenv("PSQL")
# SHP2PGSQL = os.getenv("SHP2PGSQL")


GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = Path(f"{GISDATA_FOLDER}/temp/")

BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"


with open('scripts/abbr - fips.json') as f:
    ABBR_FIPS = json.load(f)


fips_files_matching_pattern = re.compile('(?=\"tl)(.*?)(?<=>)')
find_slash_double_quote_greter_than_pattern = re.compile('[\">]')


def get_fips_from_abbr(abbr):
    state_to_fips = ABBR_FIPS
    return state_to_fips.get(abbr, 0)


def get_fips_files(url, fips):
    """
    get_fips_files () {
    local url=$1
    local fips=$2
    local files=($(wget --no-verbose -O - $url \
        | perl -nle 'print if m{(?=\"tl)(.*?)(?<=>)}g' \
        | perl -nle 'print m{(?=\"tl)(.*?)(?<=>)}g' \
        | sed -e 's/[\">]//g'))
    local matched=($(echo "${files[*]}" | tr ' ' '\n' | grep "tl_${YEAR}_${fips}"))
    echo "${matched[*]}"
}

    downloads the content from the specified URL using wget, and then uses Perl and sed commands to extract the file names from the HTML content. Specifically, the first Perl command extracts all substrings that are preceded by "tl and followed by >, and the second Perl command extracts the same substrings again. The sed command removes the "> characters from the extracted substrings. The resulting file names are stored as an array in the files variable.
    local matched=($(echo "${files[*]}" | tr ' ' '\n' | grep "tl_${YEAR}_${fips}")) filters the file names by FIPS code and the year variable. It uses the echo command to output all the file names as a space-separated string, then replaces the spaces with newlines using tr, and finally uses grep to match file names that contain the specified FIPS code and year. The resulting file names are stored as an array in the matched variable.
    echo "${matched[*]}" prints the matched file names as a space-separated string.

    files = ['tl_2021_01_test1\">', 'tl_2021_02_test2\">']
    final_files= ['tl_2021_01_test1', 'tl_2021_02_test2']
    """

    # import urllib.request
    # response = urllib.request.urlopen(url)
    # content = response.read().decode('utf-8')

    response = requests.get(url)
    content = response.text

    temp = url.replace("/", "")
    # with open(temp, "w") as f:
    #     f.write(content)
    # with open(temp, "r") as f:
    #     content = f.read()

    files = fips_files_matching_pattern.findall(content)

    files = [find_slash_double_quote_greter_than_pattern.sub('', file) for file in files]
    matched = [file for file in files if f"tl_{YEAR}_{fips}" in file]

    return matched


def download_extract(fips, section):
    os.chdir(GISDATA_FOLDER)

    current_url = f"{BASE_URL}/{section.upper()}/tl_{YEAR}_{fips}_{section}.zip"

    download(current_url)

    clear_temp(TEMP_DIR)
    os.chdir(f"{GISDATA_FOLDER}/{BASE_PATH}/{section.upper()}")
    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith(f"_{section}.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)


def download_extract_urls_of_all_files(fips, section):
    os.chdir(GISDATA_FOLDER)
    files = get_fips_files(f"{BASE_URL}/{section.upper()}", fips)

    for i in files:
        url = f"{BASE_URL}/{section.upper()}/{i}"
        download(url)

    os.chdir(GISDATA_FOLDER / BASE_PATH / section.upper())
    clear_temp(TEMP_DIR)

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith(f"_{section}.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)


def load_state_data(abbr, fips):
    db = Database()
    abbr = abbr.lower()

    # #############
    # Place
    # #############

    section = "place"
    primary_key = "plcidfp"

    print(section)
    download_extract(fips, section)

    common_sql(section, abbr, fips, primary_key, YEAR)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{abbr}_place_soundex_name ON tiger_data.{abbr}_place USING btree (soundex(name));"
    )

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_place_the_geom_gist ON tiger_data.{abbr}_place USING gist(the_geom);"
    )

    db.execute(f"ALTER TABLE tiger_data.{abbr}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.connection.commit()
    print("done")

    #############
    # Cousub
    #############

    section = "cousub"
    primary_key = "cosbidfp"

    print(section)
    download_extract(fips, section)

    common_sql(section, abbr, fips, primary_key, YEAR)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_cousub_the_geom_gist ON tiger_data.{abbr}_cousub USING gist(the_geom);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_cousub_countyfp ON tiger_data.{abbr}_cousub USING btree(countyfp);"
    )
    db.connection.commit()
    print("done")
    #############
    # Tract
    #############

    section = "tract"
    primary_key = "tract_id"

    print(section)
    download_extract(fips, section)

    common_sql(section, abbr, fips, primary_key, YEAR)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tract_the_geom_gist ON tiger_data.{abbr}_tract USING gist(the_geom);"
    )
    db.connection.commit()
    print("done")

    #############
    # Faces
    #############

    os.chdir(GISDATA_FOLDER)

    os.chdir(TEMP_DIR)
    section = "faces"
    primary_key = "gid"

    print(section)
    download_extract_urls_of_all_files(fips, section)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "faces" in z:
            dbf_files.append(z)

    common_sql(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_faces_the_geom_gist ON tiger_data.{abbr}_faces USING gist(the_geom);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_tfid ON tiger_data.{abbr}_faces USING btree (tfid);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_countyfp ON tiger_data.{abbr}_faces USING btree (countyfp);"
    )

    db.connection.commit()
    print("done")

    #############
    # FeatNames
    #############

    os.chdir(GISDATA_FOLDER)

    os.chdir(TEMP_DIR)
    section = "featnames"
    primary_key = "gid"

    print(section)
    download_extract_urls_of_all_files(fips, section)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "featnames" in z:
            dbf_files.append(z)
    common_sql(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_snd_name ON tiger_data.{abbr}_featnames USING btree (soundex(name));"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_lname ON tiger_data.{abbr}_featnames USING btree (lower(name));"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_tlid_statefp ON tiger_data.{abbr}_featnames USING btree (tlid,statefp);"
    )

    db.connection.commit()
    print("done")

    #############
    # Edges
    #############

    os.chdir(GISDATA_FOLDER)

    os.chdir(TEMP_DIR)
    section = "edges"
    primary_key = "gid"

    print(section)
    download_extract_urls_of_all_files(fips, section)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "edges" in z:
            dbf_files.append(z)

    common_sql(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_tlid ON tiger_data.{abbr}_edges USING btree (tlid);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edgestfidr ON tiger_data.{abbr}_edges USING btree (tfidr);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_tfidl ON tiger_data.{abbr}_edges USING btree (tfidl);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_countyfp ON tiger_data.{abbr}_edges USING btree (countyfp);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_edges_the_geom_gist ON tiger_data.{abbr}_edges USING gist(the_geom);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_edges_zipl ON tiger_data.{abbr}_edges USING btree (zipl);"
    )
    db.connection.commit()
    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_state_loc(CONSTRAINT pk_{abbr}_zip_state_loc PRIMARY KEY(zip,stusps,place)) INHERITS(tiger.zip_state_loc);"
    )
    db.execute(
        f"INSERT INTO tiger_data.{abbr}_zip_state_loc(zip,stusps,statefp,place) SELECT DISTINCT e.zipl, '{abbr}', '{fips}', p.name FROM tiger_data.{abbr}_edges AS e INNER JOIN tiger_data.{abbr}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.{abbr}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_zip_state_loc_place ON tiger_data.{abbr}_zip_state_loc USING btree(soundex(place));"
    )

    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")

    db.execute(f"vacuum analyze tiger_data.{abbr}_zip_state_loc;")

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_lookup_base(CONSTRAINT pk_{abbr}_zip_state_loc_city PRIMARY KEY(zip,state, county, city, statefp)) INHERITS(tiger.zip_lookup_base);"
    )
    db.execute(
        f"INSERT INTO tiger_data.{abbr}_zip_lookup_base(zip,state,county,city, statefp) SELECT DISTINCT e.zipl, '{abbr}', c.name,p.name,'{fips}'  FROM tiger_data.{abbr}_edges AS e INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '{fips}') INNER JOIN tiger_data.{abbr}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.{abbr}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_zip_lookup_base_citysnd ON tiger_data.{abbr}_zip_lookup_base USING btree(soundex(city));"
    )
    db.connection.commit()
    print("done")

    #############
    # Addr
    #############

    os.chdir(GISDATA_FOLDER)

    os.chdir(TEMP_DIR)
    section = "addr"
    primary_key = "gid"

    print(section)
    download_extract_urls_of_all_files(fips, section)

    dbf_files = []

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "addr" in z:
            dbf_files.append(z)

    common_sql(section, abbr, fips, primary_key, YEAR, dbf_files)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_least_address ON tiger_data.{abbr}_addr USING btree (least_hn(fromhn,tohn) );"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_tlid_statefp ON tiger_data.{abbr}_addr USING btree (tlid, statefp);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_zip ON tiger_data.{abbr}_addr USING btree (zip);"
    )
    db.connection.commit()
    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_state(CONSTRAINT pk_{abbr}_zip_state PRIMARY KEY(zip,stusps)) INHERITS(tiger.zip_state); "
    )
    db.execute(
        f"INSERT INTO tiger_data.{abbr}_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, '{abbr}', '{fips}' FROM tiger_data.{abbr}_addr WHERE zip is not null;"
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")

    db.connection.commit()
    print("done")

    # #############
    # # Tabblock
    # #############

    os.chdir(GISDATA_FOLDER)

    # url = f"{BASE_URL}/TABBLOCK/tl_{YEAR}_{fips}_tabblock10.zip"
    # url = f"{BASE_URL}/TABBLOCK20/tl_{YEAR}_{fips}_tabblock20.zip"

    os.chdir(TEMP_DIR)
    section = "tabblock20"
    primary_key = "geoid"

    print(section)
    download_extract(fips, section)

    common_sql(section, abbr, fips, primary_key, YEAR)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tabblock20_the_geom_gist ON tiger_data.{abbr}_tabblock20 USING gist(the_geom);"
    )

    db.connection.commit()
    print("done")

    #############
    # Block Group
    #############

    os.chdir(GISDATA_FOLDER)

    os.chdir(TEMP_DIR)
    section = "bg"
    primary_key = "bg_id"

    print(section)
    download_extract(fips, section)
    common_sql(section, abbr, fips, primary_key, YEAR)

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_bg_the_geom_gist ON tiger_data.{abbr}_bg USING gist(the_geom);"
    )

    db.connection.commit()
    print("done")


def load_states_data_caller():
    current_working_directory = os.getcwd()
    if GEOCODER_STATES == "*":
        print("'*' detected for STATES parameter. Adding data for all US states...")
        GEOCODER_STATES_LIST = list(ABBR_FIPS.keys())
    else:
        GEOCODER_STATES_LIST = GEOCODER_STATES.split(",")

    print(f"Adding US states data for {GEOCODER_STATES_LIST}")

    for state in GEOCODER_STATES_LIST:
        abbr = state
        fips = get_fips_from_abbr(abbr)

        if fips == 0:
            print(f"Error: f{abbr} is not a recognized US state abbreviation")
        else:
            print(f"Loading state data for: {abbr} {fips}")
            load_state_data(abbr, fips)

    os.chdir(current_working_directory)

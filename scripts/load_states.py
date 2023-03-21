import os

import subprocess
import zipfile
from pathlib import Path

import requests

from geocoder import Database

from dotenv import load_dotenv

from .helpers import run_shp2pgsql, clear_temp

load_dotenv(".env")

DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")

GISDATA_FOLDER = os.getenv("GISDATA_FOLDER")
PSQL = os.getenv("PSQL")
GEOCODER_STATES = os.getenv("GEOCODER_STATES")

GISDATA_FOLDER = Path(GISDATA_FOLDER)
TEMP_DIR = Path(f"{GISDATA_FOLDER}/temp/")

YEAR = "2022"
BASE_PATH = f"www2.census.gov/geo/tiger/TIGER{YEAR}"
BASE_URL = f"https://{BASE_PATH}"


SHP2PGSQL = "shp2pgsql"


def get_fips_from_abbr(abbr):
    # fips = 0
    # if abbr == "AL":
    #     fips = 1
    # elif abbr == "AK":
    #     fips = 2
    # # add more elif statements for the remaining abbreviations
    # return fips
    state_to_fips = {"MA": 25}

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
    import re
    import urllib.request

    response = urllib.request.urlopen(url)
    content = response.read().decode('utf-8')
    with open("test1.html", 'w') as f:
        f.write(content)
    pattern = '(?=\"tl)(.*?)(?<=>)'

    files = re.findall(pattern, content)

    files = [re.sub('[\">]', '', file) for file in files]
    matched = [file for file in files if f"tl_{YEAR}_{fips}" in file]
    print(matched)
    return matched


def download(url):
    response = requests.get(url, stream=True)
    total = int(response.headers.get('content-length', 0))
    print(url)
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


def download_extract(db, fips, section):
    os.chdir(GISDATA_FOLDER)

    current_url = f"{BASE_URL}/{section.upper()}/tl_{YEAR}_{fips}_{section}.zip"

    download(current_url)

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    clear_temp(TEMP_DIR)
    print(f"{GISDATA_FOLDER}/{BASE_PATH}/{section.upper()}")
    os.chdir(f"{GISDATA_FOLDER}/{BASE_PATH}/{section.upper()}")
    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith(f"_{section}.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)


def load_state_data(abbr, fips):
    db = Database()
    abbr = abbr.lower()

    # #############
    # # Place
    # #############

    download_extract(db, fips, "place")

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_place(CONSTRAINT pk_{abbr}_place PRIMARY KEY (plcidfp) ) INHERITS(tiger.place);"
    )

    run_shp2pgsql(
        f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_{fips}_place.dbf tiger_staging.{abbr}_place",
        DB_USER,
        DB_NAME,
    )

    db.execute(
        f"""
        ALTER TABLE tiger_staging.{abbr}_place RENAME geoid TO plcidfp;
        SELECT loader_load_staged_data(lower('{abbr}_place'), lower('{abbr}_place'));
        ALTER TABLE tiger_data.{abbr}_place ADD CONSTRAINT uidx_{abbr}_place_gid UNIQUE (gid);"""
    )

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{abbr}_place_soundex_name ON tiger_data.{abbr}_place USING btree (soundex(name));"
    )

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_place_the_geom_gist ON tiger_data.{abbr}_place USING gist(the_geom);"
    )

    db.execute(f"ALTER TABLE tiger_data.{abbr}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")

    #############
    # Cousub
    #############

    download_extract(db, fips, "cousub")

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_cousub(CONSTRAINT pk_{abbr}_cousub PRIMARY KEY (cosbidfp), CONSTRAINT uidx_{abbr}_cousub_gid UNIQUE (gid)) INHERITS(tiger.cousub);"
    )

    run_shp2pgsql(
        f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_{fips}_cousub.dbf tiger_staging.{abbr}_cousub",
        DB_USER,
        DB_NAME,
    )

    db.execute(
        f"""
        ALTER TABLE tiger_staging.{abbr}_cousub RENAME geoid TO cosbidfp;
        SELECT loader_load_staged_data(lower('{abbr}_cousub'), lower('{abbr}_cousub'));
        ALTER TABLE tiger_data.{abbr}_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');
        """
    )

    db.execute(
        f"ALTER TABLE tiger_staging.{abbr}_cousub RENAME geoid TO cosbidfp;SELECT loader_load_staged_data(lower('{abbr}_cousub'), lower('{abbr}_cousub')); ALTER TABLE tiger_data.{abbr}_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_cousub_the_geom_gist ON tiger_data.{abbr}_cousub USING gist(the_geom);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_cousub_countyfp ON tiger_data.{abbr}_cousub USING btree(countyfp);"
    )

    #############
    # Tract
    #############

    download_extract(db, fips, "tract")

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_tract(CONSTRAINT pk_{abbr}_tract PRIMARY KEY (tract_id) ) INHERITS(tiger.tract);"
    )

    run_shp2pgsql(
        f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_{fips}_tract.dbf tiger_staging.{abbr}_tract",
        DB_USER,
        DB_NAME,
    )

    db.execute(
        f"ALTER TABLE tiger_staging.{abbr}_tract RENAME geoid TO tract_id;  SELECT loader_load_staged_data(lower('{abbr}_tract'), lower('{abbr}_tract'));"
    )

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tract_the_geom_gist ON tiger_data.{abbr}_tract USING gist(the_geom);"
    )
    db.execute(f"VACUUM ANALYZE tiger_data.{abbr}_tract;")
    db.execute(f"ALTER TABLE tiger_data.{abbr}_tract ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")

    #############
    # Faces
    #############

    os.chdir(GISDATA_FOLDER)

    files = get_fips_files(f"{BASE_URL}/FACES", fips)
    print()

    for i in files:
        url = f"{BASE_URL}/FACES/{i}"
        response = requests.get(url, stream=True)
        zip_file_path = Path(url.lstrip("https://"))
        parent = zip_file_path.resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

        with open(zip_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

    os.chdir(GISDATA_FOLDER / BASE_PATH / "FACES")
    clear_temp(TEMP_DIR)

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_faces.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_faces(CONSTRAINT pk_{abbr}_faces PRIMARY KEY (gid)) INHERITS(tiger.faces);"
    )

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "faces" in z:
            run_shp2pgsql(
                f"shp2pgsql -D -s 4269 -g the_geom -W 'latin1' {z} tiger_staging.{abbr}_faces", DB_USER, DB_NAME
            )
            db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_faces'), lower('{abbr}_faces'));")

    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_faces_the_geom_gist ON tiger_data.{abbr}_faces USING gist(the_geom);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_tfid ON tiger_data.{abbr}_faces USING btree (tfid);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_faces_countyfp ON tiger_data.{abbr}_faces USING btree (countyfp);"
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_faces ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(f"vacuum analyze tiger_data.{abbr}_faces;")

    #############
    # FeatNames
    #############

    os.chdir(GISDATA_FOLDER)

    files = get_fips_files(f"{BASE_URL}/FEATNAMES", fips)

    for i in files:
        url = f"{BASE_URL}/FEATNAMES/{i}"
        response = requests.get(url, stream=True)
        zip_file_path = Path(url.lstrip("https://"))

        parent = zip_file_path.resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

        with open(zip_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

    os.chdir(GISDATA_FOLDER / BASE_PATH / "FEATNAMES")

    clear_temp(TEMP_DIR)

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_featnames.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_featnames(CONSTRAINT pk_{abbr}_featnames PRIMARY KEY (gid)) INHERITS(tiger.featnames);ALTER TABLE tiger_data.{abbr}_featnames ALTER COLUMN statefp SET DEFAULT '{fips}';"
    )

    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "featnames" in z:
            run_shp2pgsql(
                f"shp2pgsql -D  -s 4269 -g the_geom -W 'latin1' {z} tiger_staging.{abbr}_featnames", DB_USER, DB_NAME
            )
            db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_featnames'), lower('{abbr}_featnames'));")

    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_snd_name ON tiger_data.{abbr}_featnames USING btree (soundex(name));"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_lname ON tiger_data.{abbr}_featnames USING btree (lower(name));"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_featnames_tlid_statefp ON tiger_data.{abbr}_featnames USING btree (tlid,statefp);"
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_featnames ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(f"vacuum analyze tiger_data.{abbr}_featnames;")

    #############
    # Edges
    #############

    os.chdir(GISDATA_FOLDER)

    files = get_fips_files(f"{BASE_URL}/EDGES", fips)

    for i in files:
        url = f"{BASE_URL}/EDGES/{i}"
        response = requests.get(url, stream=True)
        zip_file_path = Path(url.lstrip("https://"))

        parent = zip_file_path.resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

        with open(zip_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

    os.chdir(GISDATA_FOLDER / BASE_PATH / "EDGES")

    clear_temp(TEMP_DIR)

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_edges.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_edges(CONSTRAINT pk_{abbr}_edges PRIMARY KEY (gid)) INHERITS(tiger.edges);"
    )
    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "edges" in z:
            run_shp2pgsql(
                f"shp2pgsql -D   -D -s 4269 -g the_geom -W 'latin1' {z} tiger_staging.{abbr}_edges", DB_USER, DB_NAME
            )
            db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_edges'), lower('{abbr}_edges'));")

    db.execute(f"ALTER TABLE tiger_data.{abbr}_edges ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
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
    db.execute(f"vacuum analyze tiger_data.{abbr}_edges;")
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

    #############
    # Addr
    #############

    os.chdir(GISDATA_FOLDER)

    files = get_fips_files(f"{BASE_URL}/ADDR", fips)

    for i in files:
        url = f"{BASE_URL}/ADDR/{i}"
        response = requests.get(url, stream=True)
        zip_file_path = Path(url.lstrip("https://"))

        parent = zip_file_path.resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

        with open(zip_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

    os.chdir(GISDATA_FOLDER / BASE_PATH / "ADDR")
    clear_temp(TEMP_DIR)

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_addr.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_addr(CONSTRAINT pk_{abbr}_addr PRIMARY KEY (gid)) INHERITS(tiger.addr);ALTER TABLE tiger_data.{abbr}_addr ALTER COLUMN statefp SET DEFAULT '{fips}';"
    )
    for z in os.listdir(os.getcwd()):
        if z.endswith(".dbf") and "addr" in z:
            run_shp2pgsql(
                f"shp2pgsql -D -s 4269 -g the_geom -W 'latin1' {z} tiger_staging.{abbr}_addr", DB_USER, DB_NAME
            )
            db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_addr'), lower('{abbr}_addr'));")

    db.execute(f"ALTER TABLE tiger_data.{abbr}_addr ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_least_address ON tiger_data.{abbr}_addr USING btree (least_hn(fromhn,tohn) );"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_tlid_statefp ON tiger_data.{abbr}_addr USING btree (tlid, statefp);"
    )
    db.execute(
        f"CREATE INDEX IF NOT EXISTS idx_tiger_data_{abbr}_addr_zip ON tiger_data.{abbr}_addr USING btree (zip);"
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_zip_state(CONSTRAINT pk_{abbr}_zip_state PRIMARY KEY(zip,stusps)) INHERITS(tiger.zip_state); "
    )
    db.execute(
        f"INSERT INTO tiger_data.{abbr}_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, '{abbr}', '{fips}' FROM tiger_data.{abbr}_addr WHERE zip is not null;"
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(f"vacuum analyze tiger_data.{abbr}_addr;")

    #############
    # Tabblock
    #############

    os.chdir(GISDATA_FOLDER)

    # url = f"{BASE_URL}/TABBLOCK/tl_{YEAR}_{fips}_tabblock10.zip"
    url = f"{BASE_URL}/TABBLOCK20/tl_{YEAR}_{fips}_tabblock20.zip"

    response = requests.get(url, stream=True)

    # total = int(response.headers.get('content-length', 0))
    zip_file_path = Path(url.lstrip("https://"))
    print(zip_file_path)

    parent = zip_file_path.resolve().parent

    parent.mkdir(parents=True, exist_ok=True)
    start = 0
    with open(zip_file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                # size = f.write(chunk)
                # start += size
                # p = (start / total) * 100
                # round_5 = 5 * round(p / 5)
                # print(f"{round_5}", end="\r")

    clear_temp(TEMP_DIR)
    os.chdir(GISDATA_FOLDER / BASE_PATH / "TABBLOCK20")
    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_tabblock20.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_tabblock(CONSTRAINT pk_{abbr}_tabblock PRIMARY KEY (tabblock_id)) INHERITS(tiger.tabblock);"
    )
    run_shp2pgsql(
        f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_{fips}_tabblock20.dbf tiger_staging.{abbr}_tabblock20",
        DB_USER,
        DB_NAME,
    )

    db.execute(
        f"ALTER TABLE tiger_staging.{abbr}_tabblock20 RENAME geoid10 TO tabblock_id;  SELECT loader_load_staged_data(lower('{abbr}_tabblock20'), lower('{abbr}_tabblock')); "
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_tabblock ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_tabblock_the_geom_gist ON tiger_data.{abbr}_tabblock USING gist(the_geom);"
    )
    db.execute(f"vacuum analyze tiger_data.{abbr}_tabblock;")

    #############
    # Block Group
    #############

    os.chdir(GISDATA_FOLDER)
    url = f"{BASE_URL}/BG/tl_{YEAR}_{fips}_bg.zip"
    response = requests.get(url, stream=True)
    # total = int(response.headers.get('content-length', 0))
    zip_file_path = Path(url.lstrip("https://"))

    parent = zip_file_path.resolve().parent

    parent.mkdir(parents=True, exist_ok=True)
    start = 0
    with open(zip_file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                # size = f.write(chunk)
                # start += size
                # p = (start / total) * 100
                # round_5 = 5 * round(p / 5)
                # print(f"{round_5}", end="\r")

    clear_temp(TEMP_DIR)
    os.chdir(GISDATA_FOLDER / BASE_PATH / "BG")
    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")

    for z in os.listdir(os.getcwd()):
        if z.startswith(f"tl_{YEAR}_{fips}") and z.endswith("_bg.zip"):
            with zipfile.ZipFile(z) as place_zip:
                place_zip.extractall(TEMP_DIR)

    os.chdir(TEMP_DIR)

    db.execute(
        f"CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_bg(CONSTRAINT pk_{abbr}_bg PRIMARY KEY (bg_id)) INHERITS(tiger.bg);"
    )
    run_shp2pgsql(
        f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' tl_{YEAR}_{fips}_bg.dbf tiger_staging.{abbr}_bg",
        DB_USER,
        DB_NAME,
    )

    db.execute(
        f"ALTER TABLE tiger_staging.{abbr}_bg RENAME geoid TO bg_id;  SELECT loader_load_staged_data(lower('{abbr}_bg'), lower('{abbr}_bg')); "
    )
    db.execute(f"ALTER TABLE tiger_data.{abbr}_bg ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(
        f"CREATE INDEX IF NOT EXISTS tiger_data_{abbr}_bg_the_geom_gist ON tiger_data.{abbr}_bg USING gist(the_geom);"
    )
    db.execute(f"vacuum analyze tiger_data.{abbr}_bg;")


def load_states_data_caller():
    if GEOCODER_STATES == "*":
        print("'*' detected for STATES parameter. Adding data for all US states...")
        GEOCODER_STATES_LIST = GEOCODER_STATES.split(",")
    else:
        GEOCODER_STATES_LIST = GEOCODER_STATES.split(",")

    for state in GEOCODER_STATES_LIST:
        abbr = state

        fips = get_fips_from_abbr(abbr)

        if fips == 0:
            print(f"Error: f{abbr} is not a recognized US state abbreviation")
        else:
            print(f"Loading state data for: {abbr} {fips}")

            load_state_data(abbr, fips)

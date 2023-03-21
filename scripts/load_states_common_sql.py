from geocoder import Database
from .helpers import run_shp2pgsql


def common_sql(section, abbr, fips, primary_key, YEAR, dbf_files=None):
    db = Database()

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    db.execute("CREATE SCHEMA tiger_staging;")
    # db.execute(
    #     f"""
    #     CREATE TABLE IF NOT EXISTS tiger_data.{abbr}_{section}
    #         (
    #             CONSTRAINT pk_{abbr}_{section} PRIMARY KEY ({primary_key}),
    #             CONSTRAINT uidx_{abbr}_{section}_gid UNIQUE (gid)) INHERITS(tiger.{section}
    #         );
    #     """
    # )
    # CREATE TABLE tiger_data.ma_tabblock20(CONSTRAINT pk_ma_tabblock20 PRIMARY KEY (geoid)) INHERITS(tiger.tabblock20);

    db.execute(
        f"CREATE TABLE tiger_data.{abbr}_{section}(CONSTRAINT pk_{abbr}_{section} PRIMARY KEY ({primary_key}) ) INHERITS(tiger.{section});"
    )

    if dbf_files is None:
        dbf_files = [f"tl_{YEAR}_{fips}_{section}.dbf"]

    for file in dbf_files:
        run_shp2pgsql(f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' {file} tiger_staging.{abbr}_{section}")
        if section in {"place", "cousub", "tract", "bg"}:
            db.execute(f"ALTER TABLE tiger_staging.{abbr}_{section} RENAME geoid TO {primary_key};")
        # why loading same thing, whasts the need other than preventing memory issue if thats an issue here
        db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_{section}'), lower('{abbr}_{section}'));")

    # db.execute(f"ALTER TABLE tiger_data.{abbr}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}');")
    db.execute(
        f"""
        ALTER TABLE tiger_data.{abbr}_{section} 
        ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}'),
        ALTER COLUMN statefp SET DEFAULT '{fips}';
        """
    )

    db.execute(f"VACUUM ANALYZE tiger_data.{abbr}_{section};")
    db.connection.commit()

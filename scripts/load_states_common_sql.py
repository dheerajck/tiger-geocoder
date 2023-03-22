from geocoder import Database
from .helpers import run_shp2pgsql


def create_section_table_and_add_data(section, abbr, fips, primary_key, YEAR, dbf_files=None):
    db = Database()

    db.execute("DROP SCHEMA IF EXISTS tiger_staging CASCADE")
    db.execute("CREATE SCHEMA tiger_staging")

    db.execute(
        f"""
        CREATE TABLE tiger_data.{abbr}_{section}(
            CONSTRAINT pk_{abbr}_{section} PRIMARY KEY ({primary_key}) 
            ) INHERITS(tiger.{section})
        """
    )

    db.execute(
        f"""
            ALTER TABLE tiger_data.{abbr}_{section} 
            ALTER COLUMN statefp SET DEFAULT '{fips}',
            ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}')
            """
    )
    if dbf_files is None:
        dbf_files = [f"tl_{YEAR}_{fips}_{section}.dbf"]

    for file in dbf_files:
        run_shp2pgsql(f"shp2pgsql -D -c -s 4269 -g the_geom -W 'latin1' {file} tiger_staging.{abbr}_{section}")
        if section in {"place", "cousub", "tract", "bg"}:
            db.execute(f"ALTER TABLE tiger_staging.{abbr}_{section} RENAME geoid TO {primary_key}")
        # why loading same thing, whats the need other than preventing memory issue if thats an issue here
        db.execute(f"SELECT loader_load_staged_data(lower('{abbr}_{section}'), lower('{abbr}_{section}'))")

    # db.execute(f"ALTER TABLE tiger_data.{abbr}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '{fips}')")
    if section in {"place", "cousub"}:
        db.execute(
            f"""
            ALTER TABLE tiger_data.{abbr}_{section} 
            ADD CONSTRAINT uidx_{abbr}_{section}_gid UNIQUE (gid)
            """
        )

    db.execute(f"VACUUM ANALYZE tiger_data.{abbr}_{section}")
    db.connection.commit()

from geocoder import Database


def create_extensions():
    db = Database()

    db.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    db.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;")
    db.execute("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;")
    db.execute("CREATE EXTENSION IF NOT EXISTS address_standardizer;")

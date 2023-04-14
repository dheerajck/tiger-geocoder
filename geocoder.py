import os
from enum import Enum

import psycopg
from dotenv import load_dotenv


load_dotenv(".env")

db_parameters = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "autocommit": True,
}


def drop_and_create_new_database():
    db_name = db_parameters["dbname"]
    temp = db_parameters.copy()
    temp["dbname"] = "postgres"
    connection = psycopg.connect(**temp)

    with connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP DATABASE {db_name}")
        except psycopg.errors.InvalidCatalogName:
            pass
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {db_name}")


class GeocodingConfidence(Enum):
    """Enum class representing the confidence levels of geocoding results."""

    EXCELLENT = "excellent"
    FAIR = "fair"
    POOR = "poor"
    NO_MATCH = "no match"


class Database:
    def __init__(self):
        """Interface to database"""
        # print(db_parameters)
        self.connection = psycopg.connect(**db_parameters)

        version = -1
        with self.connection.cursor() as cursor:
            data = cursor.execute("SELECT current_setting('server_version');").fetchone()
            version = float(data[0])

        if version < 15:
            print(version)
            raise Exception("Use postgresql 15 or new")
            exit()

    def execute(self, query, parameters=None):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, parameters)

        except psycopg.errors.UniqueViolation as e:
            pass
        except psycopg.errors.DuplicateTable as e:
            pass
        except psycopg.errors.DuplicateObject as e:
            pass
        except psycopg.Error as e:
            print(query)
            raise e

    def get_geocoded_data(self, address, pagc_normalize_address=None):
        """
        Tries to geocode given address and returns a dictionary containing the geocoded information
        if geocoding was successful
        """

        result = None
        geocoded_data = {
            'address': None,
            'latitude': None,
            'longitude': None,
            'rating': None,
            'confidence': GeocodingConfidence.NO_MATCH,
        }

        if pagc_normalize_address:
            sql_query = "SELECT pprint_addy(addy), ST_Y(geomout) As lat, ST_X(geomout) As lon, rating FROM geocode(pagc_normalize_address(%s))"

        else:
            sql_query = "SELECT pprint_addy(addy), ST_Y(geomout) As lat, ST_X(geomout) As lon, rating FROM geocode(%s)"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql_query,
                    [address],
                )
                result = cursor.fetchone()

        except psycopg.Error as e:
            raise e

        if result:
            address = result[0]
            rating = result[3]
            latitude = result[1]
            longitude = result[2]

            # Converting the rating into a confidence score
            if rating in {0, 1}:
                confidence = GeocodingConfidence.EXCELLENT
            elif rating <= 50:
                confidence = GeocodingConfidence.FAIR
            else:
                confidence = GeocodingConfidence.POOR

            geocoded_data["address"] = address
            geocoded_data["latitude"] = latitude
            geocoded_data["longitude"] = longitude
            geocoded_data["rating"] = rating
            geocoded_data["confidence"] = confidence

        return geocoded_data

    def reverse_geocode(self, latutude, longitude):
        """
        Tries to reverse geocode given coordinates and returns a dictionary containing matched streets
        if reverse geocoding was successful
        """

        result = None
        street_data = {
            'street_1': None,
            'street_2': None,
            'street_3': None,
        }

        # dont use this query in production, use parametrized version as this query is vulnerable to sql injection
        sql_query = f"""
            SELECT pprint_addy(r.addy[1]) As st1, pprint_addy(r.addy[2]) As st2, pprint_addy(r.addy[3])
            FROM reverse_geocode(ST_GeomFromText('POINT({longitude} {latutude})')) AS r"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql_query,
                )
                result = cursor.fetchone()

        except psycopg.Error as e:
            raise e

        if result:
            street_data['street_1'] = result[0]
            street_data['street_2'] = result[1]
            street_data['street_3'] = result[2]

        return street_data

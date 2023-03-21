import os
from enum import Enum
from pprint import pprint

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

    def execute(self, query, parameters=None):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, parameters)
                # return cursor.fetchall()
            except psycopg.errors.UniqueViolation as e:
                print(query, e)
            except psycopg.errors.DuplicateTable as e:
                print(query, e)
            except psycopg.errors.DuplicateObject as e:
                print(query, e)
            except psycopg.errors.UndefinedColumn as e:
                print(query, e)

            except psycopg.Error as e:
                print(query)
                raise e
                # return None

    def get_geocoded_data(self, address):
        """
        Tries to geocode given address and returns a dictionary containing the geocoded information
        if geocoding was successful
        """

        geocoded_data = {
            'address': None,
            'latitude': None,
            'longitude': None,
            'confidence': GeocodingConfidence.NO_MATCH,
        }
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT addy, ST_Y(geomout) As lat, ST_X(geomout) As lon, rating FROM geocode(%s)", [address]
            )
            result = cursor.fetchone()
        except psycopg.Error as e:
            pprint(e)
            return None
        finally:
            cursor.close()

        if result:
            address = result[0]
            rating = result[3]
            latitude = result[1]
            longitude = result[2]

            # Recreating the address
            # remove opening and closing brackets
            address = address[1:-1]
            address = address.split(',')
            address = f"{' '.join(address[:6]).strip()}, {' '.join(address[6:-1]).strip()}"

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
            geocoded_data["confidence"] = confidence

        return geocoded_data

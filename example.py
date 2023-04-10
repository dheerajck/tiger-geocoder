import psycopg
import asyncio

import os
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


async def test(address_list):
    async with await psycopg.AsyncConnection.connect(**db_parameters) as aconn:
        async with aconn.cursor() as acur:
            for address in address_list:
                await acur.execute("SELECT pprint_addy(addy), ST_Y(geomout) As lat, ST_X(geomout) As lon, rating FROM geocode(%s)", [address])
                result = await acur.fetchone()

                new_address = result[0]
                rating = result[3]
                latitude = result[1]
                longitude = result[2]

                geocoded_data = {
                    'rating': rating,
                    'address': new_address,
                    'latitude': latitude,
                    'longitude': longitude,
                }
                print(geocoded_data)


if __name__ == "__main__":
    address_list = [
        "643 Summer St, Boston, Suffolk, MA, 02210",
        '168 Mills St, Malden, MA 02148',
    ]

    asyncio.run(test(address_list))

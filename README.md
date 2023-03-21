# Steps to setup postgres postgis tiger geocoder

## First install postgresql, psql and postgis on your system


## Create a new database and a super user with the following SQL commands
```
CREATE DATABASE project_name;
CREATE USER super_user WITH SUPERUSER PASSWORD 'password';
```


## Create .env file

1. Create a new file named .env
2. Copy the contents of the .env.example file and paste them into the new .env file
3. Replace the placeholder values in the .env file with the actual values for your local environment
4. Save the changes made to the .env file, also make sure these environment variables names are not previously used for other purpose and set directly to avoid any possible collision

- UNZIPTOOL in .env should point to unzip or 7zip path
- For Unix like systems: unzip executable which is usually already installed on most Unix like platforms
- For Windows, 7-zip which is a free compress/uncompress tool you can download from http://www.7-zip.org/
- You need to iinstall shp2pgsql, its installed by default when you install PostGIS, add its path in SHP2PGSQL 
- You should install wget, wget which is a web grabber tool usually installed on most Unix/Linux, add its path in WGETTOOL systems
- If you are on windows and doesnt have wget you can download it from here https://gnuwin32.sourceforge.net/packages/wget.htm
- Check https://www.census.gov/library/reference/code-lists/ansi.html#state for state list
- The state you added should be present in "abbr - fips.json" file, if not you can add it with its fips code in "abbr - fips.json" file and see if that fips have data in their web server
- Now make sure every path in .env file is correct and installed properly by calling them from your terminal


## Install the required packages

```
pip install -r requirements.txt
```


## Create and activate a virtual environment

```
python3.11 -m venv venv
source venv/bin/activate
```


## Now run tiger_setup.py

```
python tiger_setup.py
```


## Now you can use tiger geocode, Here's an example code
```
from geocoder import Database


if __name__ == "__main__":
    db = Database()
    print(db.get_geocoded_data("60 TEMPLE PL, BOSTON, MA"))
```
**That's it! You should now be able to use TIGER geocoding in Python**
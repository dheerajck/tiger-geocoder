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

- For Unix like systems: unzip executable which is usually already
installed on most Unix like platforms
- For Windows, 7-zip which is a free compress/uncompress tool you can
download from http://www.7-zip.org/
- shp2pgsql commandline is installed by default when you install
PostGIS.
- wget which is a web grabber tool usually installed on most Unix/Linux
systems, you should install it
- if you are on winodws and doesnt have wget you can download it from
here https://gnuwin32.sourceforge.net/packages/wget.htm
- check https://www.bls.gov/respondents/mwr/electronic-data-interchange/appendix-d-usps-state-abbreviations-and-fips-codes.htm for state list



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
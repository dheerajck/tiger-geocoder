

# Steps to setup postgres postgis tiger geocoder

## First install postgresql, psql and postgis on your system


## Create a new database and user with the following SQL commands
    CREATE DATABASE project_name;
    CREATE USER new_user WITH PASSWORD 'password';
    GRANT ALL PRIVILEGES ON DATABASE project_name TO new_user;
    ALTER DATABASE project_name OWNER TO new_user;


## Now select newly created database and a super user as your active database and user
**To create a super user if needed use this sql command
```
CREATE USER super_user WITH SUPERUSER PASSWORD 'password';
```

```
\c project_name super_user
```

**Make sure to select a super user as user except in env file**


## Now install the required extensions for TIGER
**Execute the following SQL commands to install the required extensions for TIGER:**

    CREATE EXTENSION postgis;
    CREATE EXTENSION fuzzystrmatch;
    CREATE EXTENSION postgis_tiger_geocoder;
    CREATE EXTENSION address_standardizer;
    CREATE EXTENSION address_standardizer_data_us;


## Create folders for TIGER data
**Create two folders for the TIGER data with the following commands:**
```
mkdir ~/gisdata
mkdir ~/gisdata/temp
```


## Export the loader script from the command line with the following command
```
psql -U $YOUR_SUPER_USER -c "SELECT Loader_Generate_Nation_Script('sh')" -d $YOUR_DATABASE -tA > ~/gisdata/nation_script_load.sh
```


## Now open the file ~/gisdata/nation_script_load.sh and edit it to use information related to your machine
 **Read the comments here to get an idea**
```
# Temp directory created above
TMPDIR=~/gisdata/temp/
# A tool for unzipping, usually unzip on most UNIX systems.
UNZIPTOOL=unzip
# Below is not needed for sh.
# WGETTOOL="/usr/bin/wget"
# The below values are referenced by psql to decide where to connect to. See https://www.postgresql.org/docs/current/libpq-envars.html. 
export PGPORT=5432
export PGHOST=localhost
export PGUSER=your username
export PGPASSWORD=your password
export PGDATABASE=your database name
PSQL= Output of `which psql` command
# The below is entirely unnecessary other than for the fact that the rest of the script inexplicably uses a variable to refer to this app.
SHP2PGSQL=shp2pgsql
```
**Now run the loader script ~/gisdata/nation_load_script.sh**


## Load the state script
**Use https://www.bu.edu/brand/guidelines/editorial-style/us-state-abbreviations/ to find short names of states in US you need, and add them to the array.**
```
psql -U $YOUR_SUPER_USER -c "SELECT Loader_Generate_Script(ARRAY['MA'], 'sh')" -d $YOUR_DATABASE -tA > ~/gisdata/ma_load_script.sh
```


## Edit the state script ~/gisdata/ma_load_script.sh with the variables you used above

**Now run the state script ~/gisdata/ma_load_script.sh**


## Clean up the TIGER tables with the following SQL commands
```
SELECT install_missing_indexes();
vacuum (analyze, verbose) tiger.addr;
vacuum (analyze, verbose) tiger.edges;
vacuum (analyze, verbose) tiger.faces;
vacuum (analyze, verbose) tiger.featnames;
vacuum (analyze, verbose) tiger.place;
vacuum (analyze, verbose) tiger.cousub;
vacuum (analyze, verbose) tiger.county;
vacuum (analyze, verbose) tiger.state;
vacuum (analyze, verbose) tiger.zip_lookup_base;
vacuum (analyze, verbose) tiger.zip_state;
vacuum (analyze, verbose) tiger.zip_state_loc;
```


## Create .env file 

 1. Create a new file named .env
 2. Copy the contents of the .env.example file and paste them into the new .env file.
 3. Replace the placeholder values in the .env file with the actual values for your local environment.
 4. Save the changes made to the .env file, also make sure these environment variables names are not previously used for other purpose and set directly to avoid any possible collision.


## Install the required packages
```
pip install -r requirements.txt
```


## Now you can use tiger geocode, Here's an example code
```
from geocoder import Database

if __name__ == "__main__":
    db = Database()
    print(db.get_geocoded_data("60 TEMPLE PL, BOSTON, MA"))
```
**That's it! You should now be able to use TIGER geocoding in Python**

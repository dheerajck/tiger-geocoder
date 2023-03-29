# Setup

**Make sure to read below notes first as you might want to change these values**

**Command to download required data, setup postgres database and start your nominatim server**
```
docker run -it --shm-size=1g \
    -e PBF_URL=https://download.geofabrik.de/north-america/us-latest.osm.pbf \
    -e REPLICATION_URL=https://download.geofabrik.de/north-america/us-updates/ \
    -e REPLICATION_UPDATE_INTERVAL=86400 \
    -e REPLICATION_RECHECK_INTERVAL=1000 \
    -e IMPORT_US_POSTCODES=true \ 
    -e IMPORT_TIGER_ADDRESSES=true \ 
    -e IMPORT_STYLE=full \
    -e NOMINATIM_PASSWORD=very_secure_password \
    -v nominatim-data:/var/lib/postgresql/14/main \
    -v nominatim-flatnode:/nominatim/flatnode \
    -p 8080:8080 \
    --name nominatim \
    mediagis/nominatim:4.2
```

Port 8080 is the nominatim HTTP API port and 5432 is the Postgres port, which you may or may not want to expose.

If you want to check that your data import was successful, you can use the API with the following URL: http://localhost:8080/search.php?q=full%20address%20to%20geocode


## Dataset
This is the OpenStreetMap dataset third party source we are using right now https://download.geofabrik.de/ <br>
Currently we are using data United States of America dataset <br>
**PBF_URL** url to download data in pbf format is https://download.geofabrik.de/north-america/us-latest.osm.pbf <br>
**REPLICATION_URL** update url to update your data is https://download.geofabrik.de/north-america/us-updates/ <br>

Its better to first try setting up nominatim for a small dataset, so you can try setting up for New York by using these values <br>
**PBF_URL** url to download data in pbf format is https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf <br>
**REPLICATION_URL** update url to update your data is https://download.geofabrik.de/north-america/us/new-york-updates/ <br>


## Environment variables that are important for you to check and decide before downloading the data setting up db and starting nominatim

1.	Nominatim can use postcodes from an external source to improve searches that involve a GB or US postcode. This data can be optionally downloaded.

	**IMPORT_US_POSTCODES**
	Whether to download and import the US postcode dump (true) or path to US postcode dump in the container. (default: false)

2.  Nominatim is able to use the official TIGER address set to complement the OSM house number data in the US. You can add TIGER data to your own Nominatim instance by following these steps.
	The entire US adds about 10GB to your database.

	**IMPORT_TIGER_ADDRESSES**
	Whether to download and import the Tiger address data (true) or path to a preprocessed Tiger address set in the container. (default: false)

3.	To update your dataset these settings can help you
	I would suggest reading this first https://nominatim.org/release-docs/4.2.0/admin/Update/
	There are two ways to setup update of your osm data

	**UPDATE_MODE**:
	How to run replication to update nominatim data. Options: continuous/once/catch-up/none (default: none) 

	**REPLICATION_RECHECK_INTERVAL**
	How long to sleep if no update found yet (in seconds, default: 900). Requires REPLICATION_URL to be set.

	**REPLICATION_UPDATE_INTERVAL**
	How often upstream publishes diffs (in seconds, default: 86400). Requires REPLICATION_URL to be set.

	**You can also use this command to set updates which is continuous mode**
	For a list of other methods see the output of this command
	```
	docker exec -it nominatim sudo -u nominatim nominatim replication --help
	```
    The following command will keep updating the database forever
	```
	docker exec -it nominatim sudo -u nominatim nominatim replication --project-dir /nominatim
	```

4.  Setup a password to connect to the database with (default: qaIACxO6wMR3)

	**NOMINATIM_PASSWORD**=password

5.  The import style can be modified which can help to reduce size needed to load for the dataset you choose.

	**IMPORT_STYLE** (default: full)
	Available import style options are :
	admin: Only import administrative boundaries and places.
	street: Like the admin style but also adds streets.
	address: Import all data necessary to compute addresses down to house number level.
	full: Default style that also includes points of interest.
	extratags: Like the full style but also adds most of the OSM tags into the extratags column.
	See https://nominatim.org/release-docs/4.2.0/admin/Import/#filtering-imported-data for more details on those styles.


## You can set size of the tmpfs in Docker, for bigger imports (e.g. Europe) this needs to be set to at least 1GB or more. Half the size of your available RAM is recommended. (default: 64m)
**shm-size**=1g


## We are using Flatnode files when for nominatim as our dataset is large
If you plan to import a large dataset (e.g. Europe, North America, planet), you should also enable flatnode storage of node locations. 
With this setting enabled, node coordinates are stored in a simple file instead of the database. This will save you import time and disk storage. Add to your .env:
There should be at least 75GB of free space.

Nominatim uses https://osm2pgsql.org/ to import data from .osm/.pbf files into a Postgresql database
osm2pgsql is very much optimized and the flatnode file is one way to save memory. Usually a planet_osm_nodes gets created which stores a mapping of nodes to ways and ends up with billions of rows in the database.
The flatnode files stores the same but much more compact and memory efficient. https://osm2pgsql.org/doc/manual.html#advanced-topics
It makes sense to use the flatnode file for large countries, continents, the whole planet.

##  Persistent container data 
If you want to keep your imported data across deletion and recreation of your container, make the following folder a volume:

These are addd with -v flag in docker run -it command
/var/lib/postgresql/14/main is the storage location of the Postgres database & holds the state about whether the import was successful
/nominatim/flatnode is the storage location of the flatnode file.







# Current and Historical Temperatures
## Examine 40-year temperature trends for locations in the United States

The modules in this repository download and compile temperature data for U. S. locations across a >40-year span.  When the user provides a location, the [API](https://en.wikipedia.org/wiki/Api) returns the current temperature and (optionally) temperatures on the same day for the prior 40+ years so that temperature trends can be analyzed and visualized.

Temperature data are from the Integrated Surface Database (ISD) Lite from the National Oceanic and Atmospheric Administration (NOAA) and the National Centers for Environmental Information (NCEI).


### Current Weather example

Input:

```code
{
  "latitude": 30,
  "longitude": -100
}
```

Output:

```code
{
  "valid_response": true,
  "temperature_celsius": 27.222222222222225,
  "radar_station": "KDFX",
  "coordinates_city": "Rocksprings",
  "coordinates_state": "TX",
  "error_message": ""
}
```


### Current and Historical Weather example

Input:

```code
{
  "latitude": 40,
  "longitude": -80
}
```

Output:

```code
{
  "current_temperature_celsius": 11.666666666666668,
  "current_station": "KPBZ",
  "current_city": "Morgantown",
  "current_state": "WV",
  "current_error_message": "",
  "distance_to_station_kilometers": 39.444955295990184,
  "annual_timestamp": [
    "1980-02-21T13:00:00",
    "1980-02-22T00:00:00",
    "1980-02-22T01:00:00",
    "1980-02-22T02:00:00",
    ...
    "2023-02-22T22:00:00",
    "2023-02-22T23:00:00",
    "2023-02-23T01:00:00"
  ],
  "annual_temperature_celsius": [
    5.6,
    7.8,
    7.2,
    6.7,
    ...
    17.2,
    16.7,
    15
  ],
  "annual_usaf_station_id": 724176,
  "annual_wban_station_id": 13736,
  "annual_station_name": "MGTN RGNL-W L B HART FD AP",
  "annual_station_state": "WV",
  "annual_station_call": "KMGW",
  "annual_station_latitude": 39.650001525878906,
  "annual_station_longitude": -79.9209976196289,
  "annual_station_elevation_meters": 373.6000061035156
}
```

## Repository modules
### Step 1:  Identify weather stations

Directory: ```src/step01_identify_stations```

Extract data from Integrated Surface Database Station History text file ```isd-history.txt``` to create a table of weather station data, including stations' names, IDs, locations, and time periods of data collection

```isd-history.txt``` was downloaded from:

    ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt


### Step 2:  Download weather station data

Directory: ```src/step02_download```

Download Integrated Surface Database (ISD) Lite files containing historical weather data from the National Oceanic and Atmospheric Administration (NOAA) and the National Centers for Environmental Information (NCEI) 

### Step 3:  Compile downloaded data

Directory: ```src/step03_process_tables```

Downloaded data tables provide weather data for each station for each year (i.e., one table per station-year)

Compile, organize, and save data two ways:  

1. Compile the data so that each station has one file that encompasses all years  
2. Provide a single file that includes all years for all stations  


### Step 4:  Set up logging configuration

Directory: ```src/step04_set_up_log_config```

Set up a JSON logging configuration file so that responses from the National Weather Service (NWS) API are logged.


### Step 5:  Run API

Directory: ```src/step05_api```

Request current and historical weather information -- temperature, specifically -- from the National Weather Service (NWS) API and the Integrated Surface Data (ISD) Lite data set, respectively, for a given pair of latitude and longitude coordinates

The temperature data includes today's current temperature and all temperatures recorded on the same day of the year for prior years, as well as associated data (e.g., weather station location and elevation); such data can be used to identify and visualize annual temperature trends

The ISD-Lite data set has been filtered to include only weather stations that were active in the prior year and have a long history of continual observations


## Run modules
### Steps 1 - 4

The modules for Steps 1, 2, 3, and 4 may be run with their respective Anaconda environments as specified in the ```environment.yml``` files in their directories.

From within their directories, you can [run these commands](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html), substituting the appropriate names for the all-caps labels:

```code
conda env create -f environment.yml

conda activate ENVIRONMENT_NAME

python PYTHON_FILE_NAME.py
```

The specific commands follow:

### Step 1

```code
conda env create -f environment.yml

conda activate data_processing06

python identify_stations.py
```

### Step 2

```code
conda env create -f environment.yml

conda activate async_download04

python download_isd_lite.py
```

### Step 3

```code
conda env create -f environment.yml

conda activate data_processing06

python process_data_tables.py
```

### Step 4

```code
conda env create -f environment.yml

conda activate geodata_processing04

python set_up_log_configuration.py
```

### Step 5

The module for Step 5 may be run with either the Anaconda environment specified in its directory or with Poetry.  Either method must be run from the top-level project directory.

The commands for running Step 5 with Anaconda are:

```code
conda env create -f src/step05_api/environment.yml

conda activate geodata_processing04

uvicorn src.step05_api.api:app
```

The [commands](https://python-poetry.org/docs/basic-usage/) for running Step 5 with Poetry are:

```code
poetry install

poetry shell

poetry run uvicorn src.step05_api.api:app
```

For running in both Anaconda and Poetry environments, you can [test the API locally](https://fastapi.tiangolo.com/tutorial/first-steps/) for either current temperature or for current-and-historical temperatures by pointing your browser to:

http://127.0.0.1:8000/docs

or

http://127.0.0.1:8000/redoc

You can also call the API directly from the terminal.  For example:

```code
curl -X 'POST' \
  'http://127.0.0.1:8000/current_temperature' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "latitude": 40,
  "longitude": -120
}'
```

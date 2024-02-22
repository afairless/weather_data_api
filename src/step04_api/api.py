#! /usr/bin/env python3

import datetime
import requests
import polars as pl
import geopy.distance as gd
from time import sleep
from pathlib import Path
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Callable


class Coordinates(BaseModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class CurrentWeather(BaseModel):
    valid_response: bool
    # temperature in Fahrenheit degrees
    # tests somewhat above highest- and below lowest-ever recorded temperatures
    temperature_celsius: float = Field(ge=-150, le=150, default=-9999)
    radar_station: str = Field(min_length=2, max_length=4, default='')
    coordinates_city: str = Field(max_length=50, default='')
    coordinates_state: str = Field(min_length=2, max_length=2, default='')
    error_message: str = ''


class AnnualWeather(BaseModel):

    # validation checks are omitted from attributes derived from 
    #   'CurrentWeather', because when they are at their default values, 
    #   explicitly assigning them here subjects them to failing validation
    #   (instead of skipping validation of default values, which is the usual 
    #   behavior for Pydantic BaseModel)

    current_temperature_celsius: float
    current_station: str
    current_city: str
    current_state: str
    current_error_message: str = ''

    distance_to_station_kilometers: float = Field(ge=0, le=21_000)
    annual_timestamp: list[datetime.datetime]
    annual_temperature_celsius: list[float]
    annual_usaf_station_id: int = Field(ge=0, lt=1_000_000)
    annual_wban_station_id: int = Field(ge=0, lt=100_000)
    annual_station_name: str = Field(max_length=100)
    annual_station_state: str = Field(min_length=2, max_length=2)
    annual_station_call: str = Field(max_length=4)
    annual_station_latitude: float = Field(ge=-90, le=90)
    annual_station_longitude: float = Field(ge=-180, le=180)
    # tests somewhat above highest (Mount Everest) and below lowest (Dead Sea) 
    #   surface elevations
    annual_station_elevation_meters: float = Field(ge=-440, le=8850)


def convert_fahrenheit_to_celsius(fahrenheit: float) -> float:
    """
    Convert given temperature in degrees Fahrenheit to degrees Celsius
    """
    return 5/9 * (fahrenheit - 32)


def identify_closest_coordinates(
    input_coordinates: tuple[float, float], coordinates_df: pl.DataFrame
    ) -> tuple[int, float]:
    """
    Given a latitude-longitude coordinate pair ('input_coordinates') and a 
        dataframe of such pairs ('coordinates_df'), identify which pair in 
        'coordinates_df' is closest to 'input_coordinates' and return the 
        corresponding row index and distance in kilometers
    Assumes that 'coordinates_df' has a column of latitudes named 'lat' and a 
        column of longitudes named 'lon'

    Example of 'coordinates_df':

        >>> coordinates_df[['lat', 'lon']].head()
        shape: (5, 2)
        ┌───────────┬─────────────┐
        │ lat       ┆ lon         │
        │ ---       ┆ ---         │
        │ f32       ┆ f32         │
        ╞═══════════╪═════════════╡
        │ 34.293999 ┆ -116.147003 │
        │ 71.287003 ┆ -156.738998 │
        │ 70.637001 ┆ -160.013    │
        │ 70.191002 ┆ -148.479996 │
        │ 70.134003 ┆ -143.576996 │
        └───────────┴─────────────┘
    """


    # INPUT PRE-CHECKS
    ##################################################

    assert len(input_coordinates) == 2

    assert len(coordinates_df) > 0
    assert 'lat' in coordinates_df.columns
    assert 'lon' in coordinates_df.columns
    assert coordinates_df['lat'].dtype.is_numeric()
    assert coordinates_df['lon'].dtype.is_numeric()


    # IDENTIFY CLOSEST COORDINATES
    ##################################################

    coordinates_srs = coordinates_df[['lat', 'lon']].to_struct()

    min_distance = float('inf')
    min_distance_idx = -1

    for i, e in enumerate(coordinates_srs):

        distance = (
            gd.distance(input_coordinates, (e['lat'], e['lon'])).kilometers)

        if distance < min_distance:
            min_distance_idx, min_distance = i, distance

    return min_distance_idx, min_distance 


def retry_request(
    retry_counter: int, sleep_duration: int, 
    request_func: Callable, *args, **kwargs) -> tuple[requests.Response, int]:
    """
    Retry web request function ('request_func') a maximum of 'retry_counter' 
        number of times with an interval of 'sleep_duration' (in seconds) 
        between each try
    If a response status code of 200 is received, returns response immediately
    """

    if retry_counter < 1:
        retry_counter = 0

    for i in range(retry_counter):
        response = request_func(*args, **kwargs)
        if response.ok:
            return response, i
        elif i < (retry_counter-1):
            sleep(sleep_duration)
        else:
            return response, i


def request_station_information(
    coordinates: tuple[float, float]) -> requests.Response:
    """
    Request weather station information from the National Weather Service API

    API documentation:
        https://www.weather.gov/documentation/services-web-api
    """

    base_url = 'https://api.weather.gov/points/'
    input_coordinates_str = ','.join([str(e) for e in coordinates])
    url = base_url + input_coordinates_str 
    response = requests.get(url)
    return response


def filter_df_by_id_and_date(
    df: pl.DataFrame, output_colnames: list[str], 
    id: int, id_colname: str, date_colname: str,
    target_datetime: datetime.datetime) -> pl.DataFrame:
    """
    Filters a given dataframe by an ID number ('id') and a date 
        ('target_datetime') and returns the specified columns
        ('output_colnames') 
    The date filtering includes all dates in 'df' with the same month and day
        as 'target_datetime' for all years present in 'df', as well as the row 
        immediately preceding and the row immediately following each of those 
        dates; this is particularly useful when the same ID numbers in 'df' are 
        all on consecutive rows and 'date_colname' is sorted within each ID 
        number group (though these conditions are not enforced)

    Example input 'df':
        ┌─────────────────────┬─────┐
        │ timestamp           ┆ id  │
        │ ---                 ┆ --- │
        │ datetime[μs]        ┆ i64 │
        ╞═════════════════════╪═════╡
        │ 2020-01-01 00:00:00 ┆ 8   │
        │ 2020-01-01 12:00:00 ┆ 8   │
        │ 2020-01-02 00:00:00 ┆ 8   │
        │ 2020-01-02 12:00:00 ┆ 8   │
        │ 2020-01-03 00:00:00 ┆ 8   │
        │ 2020-01-03 12:00:00 ┆ 8   │
        │ 2020-01-01 00:00:00 ┆ 5   │
        │ 2020-01-01 12:00:00 ┆ 5   │
        │ 2020-01-02 00:00:00 ┆ 5   │
        │ 2020-01-02 12:00:00 ┆ 5   │
        │ 2020-01-03 00:00:00 ┆ 5   │
        │ 2020-01-03 12:00:00 ┆ 5   │
        └─────────────────────┴─────┘

    Example output 'df1' when 
        target date is January 2,
        'id' = 5, 
        'id_colname' = 'id', 
        'date_colname' = 'timestamp', and
        'output_colnames' = ['timestamp', 'id']:
        ┌─────────────────────┬─────┐
        │ timestamp           ┆ id  │
        │ ---                 ┆ --- │
        │ datetime[μs]        ┆ i64 │
        ╞═════════════════════╪═════╡
        │ 2020-01-01 12:00:00 ┆ 5   │
        │ 2020-01-02 00:00:00 ┆ 5   │
        │ 2020-01-02 12:00:00 ┆ 5   │
        │ 2020-01-03 00:00:00 ┆ 5   │
        └─────────────────────┴─────┘
    """


    # INPUT PRE-CHECKS
    ##################################################

    assert len(df) > 0
    assert len(output_colnames) > 0

    assert id_colname in df.columns
    assert date_colname in df.columns

    for e in output_colnames:
        assert e in df.columns

    assert id in df[id_colname]


    # FILTER DATAFRAME
    ##################################################

    target_date_idx_df = (
        df 
        .with_row_index()
        .filter(
            pl.col(id_colname) == id,
            pl.col(date_colname).dt.month() == target_datetime.month,
            pl.col(date_colname).dt.day() == target_datetime.day)
        .select(
            pl.col(date_colname).dt.date().alias(date_colname),
            pl.col('index')))

    assert target_date_idx_df['index'].is_sorted()

    pre_target_date_start_idx_df = (
        target_date_idx_df.group_by(pl.col(date_colname)).first()['index'] - 1)
    post_target_date_end_idx_df = (
        target_date_idx_df.group_by(pl.col(date_colname)).last()['index'] + 1)

    idx = target_date_idx_df['index'] 
    idx.append(pre_target_date_start_idx_df).append(post_target_date_end_idx_df)
    
    df1 = df[idx, :].select(output_colnames).sort(date_colname)

    return df1


def get_historical_temperatures(
    filepath: Path, station_id: int) -> pl.DataFrame:

    temperature_df = pl.read_parquet(filepath)
    id_colname = 'station'

    if id_colname not in temperature_df.columns:
        temperature_df = temperature_df.with_columns(
            pl.lit(station_id).alias(id_colname))

    colnames = ['timestamp', 'temperature']
    now = datetime.datetime.now()

    # assert temperature_df['station'].is_sorted()
    df = filter_df_by_id_and_date(
        temperature_df, colnames, station_id, id_colname, 'timestamp', now)

    return df


def provide_response_error_messages() -> tuple[str, str, str, str, str]:
    """
    Defines error messages for user when calls to National Weather Service API
        fail to retrieve expected information
    Defined in function separate from 'request_current_weather' so that error
        messages can be easily used in unit tests in accordance with DRY
    """

    forecast_url_missing_message = (
        'Valid response from current weather API, but forecast URL not in '
        'expected place in response.')
    invalid_input_coordinates_message = (
        'Invalid input coordinates.  '
        'The first coordinate should be latitude; '
        'the second should be longitude.  '
        'Ensure that the coordinates are within the United States.')
    invalid_weather_api_response_message = (
        'Invalid response from current weather API.')
    temperature_missing_message = (
        'Valid response from current weather API, but temperature not '
        'in expected place in response.')
    location_missing_message = (
        'Valid response from current weather API, but location '
        'information is incomplete.')

    return (
        forecast_url_missing_message, invalid_input_coordinates_message,
        invalid_weather_api_response_message, temperature_missing_message,
        location_missing_message)


def request_current_weather(coordinates: Coordinates) -> CurrentWeather:
    """
    Request current weather information from the National Weather Service (NWS) 
        API for a given pair of latitude and longitude coordinates
    If information is unavailable, return appropriate messages to user
    Two requests are submitted to the National Weather Service API:
        1) The first request submits the coordinates and receives information
            about the closest weather station, including a URL that can be used
            to access the weather forecast for that weather station
        2) The second request submits the URL obtained during the first request 
            and receives weather forecast information, including the current
            temperature

    NOTE:  NWS API sometimes returns valid weather station and 'forecast_url' 
        for coordinates that are far from the United States, which can cause
        inconsistent data downstream of this function; it would take more
        experimentation to determine the conditions that do and do not trigger
        the 'invalidpoint' response from the API
    """

    # SET UP REQUEST RETRY PARAMETERS AND ERROR MESSAGES
    ##################################################

    retry_n = 3
    retry_delay = 4

    error_messages = provide_response_error_messages()
    forecast_url_missing_message = error_messages[0]
    invalid_input_coordinates_message = error_messages[1]
    invalid_weather_api_response_message = error_messages[2]
    temperature_missing_message = error_messages[3]
    location_missing_message = error_messages[4]


    # SUBMIT AND HANDLE FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    input_coordinates = (coordinates.latitude, coordinates.longitude)
    station_response, _ = retry_request(
        retry_n, retry_delay, request_station_information, input_coordinates)

    if station_response.ok:
        try:
            forecast_url = station_response.json()['properties']['forecast']
        except:
            current_weather = CurrentWeather(
                valid_response = False,
                error_message = forecast_url_missing_message)
            return current_weather 

    else:

        current_weather = CurrentWeather(valid_response = False)

        error_type = station_response.json()['type']

        if 'invalidpoint' in error_type.lower():
            current_weather.error_message = invalid_input_coordinates_message 
        else:
            current_weather.error_message = invalid_weather_api_response_message

        return current_weather 


    # SUBMIT AND HANDLE SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_response, _ = retry_request(
        retry_n, retry_delay, requests.get, forecast_url)

    if forecast_response.ok:

        current_weather = CurrentWeather(valid_response = True)

        try:
            current_temperature = (
                forecast_response.json()['properties']['periods'][0]
                ['temperature'])
        except:
            current_weather.error_message = temperature_missing_message 
            return current_weather 

        try:
            city = (
                station_response.json()['properties']['relativeLocation']
                ['properties']['city'])
            state = (
                station_response.json()['properties']['relativeLocation']
                ['properties']['state'])
            radar_station = (
                station_response.json()['properties']['radarStation'])
            # distance = (
            #     station_response.json()['properties']['relativeLocation']
            #     ['properties']['distance']['value'])

        except:

            current_weather.temperature_celsius = (
                convert_fahrenheit_to_celsius(current_temperature))
            current_weather.error_message = location_missing_message 

            return current_weather 

        current_weather.temperature_celsius = (
            convert_fahrenheit_to_celsius(current_temperature))
        current_weather.radar_station = radar_station 
        current_weather.coordinates_city = city
        current_weather.coordinates_state = state

    else:
        current_weather = CurrentWeather(valid_response = False)

    return current_weather 


app = FastAPI()


@app.post('/current_temperature', response_model=CurrentWeather)
async def get_current_temperature(coordinates: Coordinates) -> CurrentWeather:

    current_weather = request_current_weather(coordinates)
    return current_weather 


def get_station_df(station_filepath: Path) -> pl.DataFrame:
    """
    Load weather station information table 
    Separated into own function to allow mocking in unit tests
    """
    station_df = pl.read_parquet(station_filepath)
    return station_df


@app.post('/annual_temperature', response_model=AnnualWeather)
def get_historical_and_current_temperatures(
    coordinates: Coordinates) -> AnnualWeather:
    """
    Request current and historical weather information -- temperature, 
        specifically -- from the National Weather Service (NWS) API and the 
        Integrated Surface Data (ISD) Lite data set, respectively, for a given 
        pair of latitude and longitude coordinates
    The temperature data includes today's current temperature and all 
        temperatures recorded on the same day of the year for prior years, as 
        well as associated data (e.g., weather station location and elevation); 
        such data can be used to identify and visualize annual temperature 
        trends
    For further information on requesting current weather data from NWS API, see
        the function 'request_current_weather'
    The ISD-Lite data set has been filtered to include only weather stations 
        that were active in the prior year and have a long history of
        continual observations
    """


    # LOAD WEATHER STATION TABLE
    ##################################################

    station_filepath = Path.cwd() / 'output' / 'stations_to_download.parquet'
    station_df = get_station_df(station_filepath)


    # IDENTIFY STATION NEAREST TO USER-PROVIDED COORDINATES
    ##################################################

    input_coordinates = (coordinates.latitude, coordinates.longitude)
    min_distance_idx, min_distance_kilometers = identify_closest_coordinates(
        input_coordinates, station_df[['lat', 'lon']])


    # LOAD HISTORICAL TEMPERATURES FROM SELECTED STATION
    ##################################################

    station_data = station_df[min_distance_idx, :].to_dict()
    station_id = int(
        str(station_data['usaf'][0]) + str(station_data['wban'][0]))

    # DEPRECATED:  loading entire, large table
    # historical_temperature_filepath = (
    #     Path.home() / 'Documents' / 'isd_lite_compiled' / 
    #     'station_temperatures.parquet')
    # temperature_df = get_historical_temperatures(
    #     historical_temperature_filepath, station_id)

    station_temperature_filename = (
        str(station_data['usaf'][0]) + '-' + 
        str(station_data['wban'][0]) + '.parquet')
    station_temperature_filepath = (
        Path.home() / 'Documents' / 'isd_lite_compiled' / 
        station_temperature_filename)
    temperature_df = get_historical_temperatures(
        station_temperature_filepath, station_id)

    # temperature is encoded with factor of 10x and with precision of one-tenth 
    #   of a degree, so divide to convert to degrees
    temperature_df = temperature_df.with_columns(
        pl.col('timestamp'),
        (pl.col('temperature') / 10).round(1))
    # temperature_df['timestamp'].dt.date().unique().sort()

    temperature_dict = temperature_df.to_dict()


    # REQUEST CURRENT TEMPERATURE 
    ##################################################

    station_coordinates = Coordinates(
        latitude=station_data['lat'][0], 
        longitude=station_data['lon'][0])

    current_weather = request_current_weather(station_coordinates)
    # the 'radar_station' from 'current_weather' and the 'call' from 
    #   'station_data' don't necessarily match, even though coordinates from
    #   'station_data' are used to obtain 'current_weather'


    # COMPILE HISTORICAL AND CURRENT TEMPERATURES AND RELATED DATA
    ##################################################

    # the station call string is sometimes received as a null
    if not station_data['call'][0]: 
        station_call = ''
    else:
        station_call = station_data['call'][0]

    annual_weather = AnnualWeather(
        current_temperature_celsius = current_weather.temperature_celsius,
        current_station = current_weather.radar_station,
        current_city = current_weather.coordinates_city,
        current_state = current_weather.coordinates_state,
        current_error_message = current_weather.error_message,
        distance_to_station_kilometers = min_distance_kilometers,
        annual_timestamp = temperature_dict['timestamp'].to_list(),
        annual_temperature_celsius = temperature_dict['temperature'].to_list(),
        annual_usaf_station_id = int(station_data['usaf'][0]),
        annual_wban_station_id = int(station_data['wban'][0]),
        annual_station_name = station_data['station_name'][0],
        annual_station_state = station_data['st'][0],
        annual_station_call = station_call,
        annual_station_latitude = station_data['lat'][0],
        annual_station_longitude = station_data['lon'][0],
        annual_station_elevation_meters = station_data['elev(m)'][0])

    return annual_weather

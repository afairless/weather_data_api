 
import pytest
import pytest_mock
import datetime
import polars as pl
import geopy.distance as gd
from dataclasses import dataclass

from src.step04_api.api import (
    convert_fahrenheit_to_celsius,
    identify_closest_coordinates,
    filter_df_by_id_and_date,
    request_station_information,
    Coordinates,
    CurrentWeather,
    AnnualWeather,
    request_current_weather,
    provide_response_error_messages,
    get_historical_and_current_temperatures,
    )


def test_convert_fahrenheit_to_celsius_01():
    """
    Convert temperature:  test water freezing point
    """

    fahrenheit_temperature = 32
    correct_result = 0
    result = convert_fahrenheit_to_celsius(fahrenheit_temperature)
    assert result == correct_result


def test_convert_fahrenheit_to_celsius_02():
    """
    Convert temperature:  test water boiling point
    """

    fahrenheit_temperature = 212
    correct_result = 100
    result = convert_fahrenheit_to_celsius(fahrenheit_temperature)
    assert result == correct_result


def test_convert_fahrenheit_to_celsius_03():
    """
    Convert temperature:  test equal point between the two scales
    """

    fahrenheit_temperature = -40
    correct_result = fahrenheit_temperature
    result = convert_fahrenheit_to_celsius(fahrenheit_temperature)
    assert result == correct_result


def test_identify_closest_coordinates_01():
    """
    Test empty input
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame()

    with pytest.raises(AssertionError):
        result = identify_closest_coordinates(input_coordinates, coordinates_df)


def test_identify_closest_coordinates_02():
    """
    Test empty input
    """

    input_coordinates = ()
    coordinates_df = pl.DataFrame({
        'lat': [  34.,   71.,   70.,   72.,   69.],
        'lon': [-116., -156., -160., -148., -143.]})

    with pytest.raises(AssertionError):
        result = identify_closest_coordinates(input_coordinates, coordinates_df)


def test_identify_closest_coordinates_03():
    """
    Test input table with wrong column name
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame({
        'aaa': [  34.,   71.,   70.,   72.,   69.],
        'lon': [-116., -156., -160., -148., -143.]})

    with pytest.raises(AssertionError):
        result = identify_closest_coordinates(input_coordinates, coordinates_df)


def test_identify_closest_coordinates_04():
    """
    Test input table with wrong data type
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame({
        'lat': [ '34',  '71',  '70',  '72',  '69'],
        'lon': [-116., -156., -160., -148., -143.]})

    with pytest.raises(AssertionError):
        result = identify_closest_coordinates(input_coordinates, coordinates_df)


def test_identify_closest_coordinates_05():
    """
    Test valid input of data type 'float'
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame({
        'lat': [  34.,   71.,   70.,   72.,   69.],
        'lon': [-116., -156., -160., -148., -143.]})

    result = identify_closest_coordinates(input_coordinates, coordinates_df)

    correct_result = (2, 0.)
    assert result == correct_result


def test_identify_closest_coordinates_06():
    """
    Test valid input of data type 'float' with different order
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame({
        'lat': [  70.,   34.,   71.,   72.,   69.],
        'lon': [-160., -116., -156., -148., -143.]})

    result = identify_closest_coordinates(input_coordinates, coordinates_df)

    correct_result = (0, 0.)
    assert result == correct_result


def test_identify_closest_coordinates_07():
    """
    Test valid input of data type 'float' with non-zero distance
    """

    input_coordinates = (70., -160.)
    coordinates_df = pl.DataFrame({
        'lat': [  34.,   71.,   70.1,   72.,   69.],
        'lon': [-116., -156., -160. , -148., -143.]})

    result = identify_closest_coordinates(input_coordinates, coordinates_df)

    correct_result = (2, 11.156265636145449)
    assert result == correct_result


def test_identify_closest_coordinates_08():
    """
    Test valid input of data type 'integer'
    """

    input_coordinates = (70, -160)
    coordinates_df = pl.DataFrame({
        'lat': [  34,   71,   70,   72,   69],
        'lon': [-116, -156, -160, -148, -143]})

    result = identify_closest_coordinates(input_coordinates, coordinates_df)

    correct_result = (2, 0)
    assert result == correct_result


def test_filter_df_by_id_and_date_01():
    """
    Test empty input
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['timestamp', 'id']

    df = pl.DataFrame()

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_02():
    """
    Test empty input
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = []

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'id': ids})

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_03():
    """
    Test input dataframe with missing column name
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['timestamp', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'incorrect_column_name': ids})

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_04():
    """
    Test input dataframe with missing column name
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['timestamp', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'incorrect_column_name': datetimes,
        'id': ids})

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_05():
    """
    Test input dataframe with missing column name
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['incorrect_column_name', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'id': ids})

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_06():
    """
    Test input dataframe with missing ID number
    """

    id = 3
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['timestamp', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'id': ids})

    with pytest.raises(AssertionError):
        result = filter_df_by_id_and_date(
            df, colnames, id, 'id', 'timestamp', target_day)


def test_filter_df_by_id_and_date_07():
    """
    Test valid input
    """

    id = 1
    target_day = datetime.datetime(2000, 1, 2, 0, 0)
    colnames = ['timestamp', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'id': ids})

    result = filter_df_by_id_and_date(
        df, colnames, id, 'id', 'timestamp', target_day)

    correct_timestamp = [
        datetime.datetime(2019,  1,  1, 16, 0),
        datetime.datetime(2019,  1,  2,  0, 0),
        datetime.datetime(2019,  1,  2,  8, 0),
        datetime.datetime(2019,  1,  2, 16, 0),
        datetime.datetime(2019,  1,  3,  0, 0),
        datetime.datetime(2020,  1,  1, 16, 0),
        datetime.datetime(2020,  1,  2,  0, 0),
        datetime.datetime(2020,  1,  2,  8, 0),
        datetime.datetime(2020,  1,  2, 16, 0),
        datetime.datetime(2020,  1,  3,  0, 0)]

    correct_result = pl.DataFrame({
        'timestamp': correct_timestamp,
        'id': [1] * len(correct_timestamp)})

    assert result.equals(correct_result)


def test_filter_df_by_id_and_date_08():
    """
    Test valid input where date range spans multiple calendar years
    """

    id = 2
    target_day = datetime.datetime(1000, 1, 1, 0, 0)
    colnames = ['timestamp', 'id']

    datetimes = pl.datetime_range(
        datetime.datetime(2018, 12, 20, 0, 0), 
        datetime.datetime(2020,  1, 12, 0, 0), 
        '8h', eager=True)

    ids = [1] * len(datetimes) + [2] * len(datetimes)
    datetimes.append(datetimes)

    df = pl.DataFrame({
        'timestamp': datetimes,
        'id': ids})

    result = filter_df_by_id_and_date(
        df, colnames, id, 'id', 'timestamp', target_day)

    correct_timestamp = [
        datetime.datetime(2018, 12, 31, 16, 0),
        datetime.datetime(2019,  1,  1,  0, 0),
        datetime.datetime(2019,  1,  1,  8, 0),
        datetime.datetime(2019,  1,  1, 16, 0),
        datetime.datetime(2019,  1,  2,  0, 0),
        datetime.datetime(2019, 12, 31, 16, 0),
        datetime.datetime(2020,  1,  1,  0, 0),
        datetime.datetime(2020,  1,  1,  8, 0),
        datetime.datetime(2020,  1,  1, 16, 0),
        datetime.datetime(2020,  1,  2,  0, 0)]

    correct_result = pl.DataFrame({
        'timestamp': correct_timestamp,
        'id': [2] * len(correct_timestamp)})

    assert result.equals(correct_result)


@pytest.mark.skip(reason='calls external API')
def test_request_station_information_01():
    coordinates = (40, -88)
    response = request_station_information(coordinates)
    assert response.ok


@pytest.mark.skip(reason='used only to verify mock operation')
def test_request_station_information_02(mocker: pytest_mock.MockFixture):
    mocker.patch('src.step04_api.api.requests.get')
    coordinates = (40, -88)
    response = request_station_information(coordinates)
    assert response.ok


def mock_sleep(dummy):
    pass


@pytest.mark.skip(reason='calls external API; used to create mock response')
def test_request_current_weather_01():
    """
    Test valid response from external API
    """

    correct_result = CurrentWeather(
        valid_response = True,
        temperature_celsius = 23,
        radar_station = 'KILX',
        coordinates_city = 'Homer',
        coordinates_state = 'IL',
        error_message = '')

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    assert result.valid_response == correct_result.valid_response 

    # temperature returned in Fahrenheit degrees
    # tests somewhat above highest- and below lowest-ever recorded temperatures
    assert result.temperature_celsius > -150
    assert result.temperature_celsius <  150

    assert result.coordinates_city == correct_result.coordinates_city  
    assert result.radar_station == correct_result.radar_station
    assert result.coordinates_state == correct_result.coordinates_state   
    assert result.error_message == correct_result.error_message


def test_request_current_weather_02(mocker: pytest_mock.MockFixture):
    """
    Test valid input and responses
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_url = 'https://api.weather.gov/points/000,000'
    forecast_url = 'https://api.weather.gov/gridpoints/KILX/000,000/forecast'
    station_json = {
        'properties': {
            'forecast': forecast_url,
            'relativeLocation': {
                'properties': {'city': 'Homer', 'state': 'IL'}},
            'radarStation': 'KILX'}}

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = station_url
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = forecast_url
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    correct_result = CurrentWeather(
        valid_response = True,
        temperature_celsius = convert_fahrenheit_to_celsius(23),
        radar_station = 'KILX',
        coordinates_city = 'Homer',
        coordinates_state = 'IL',
        error_message = '')

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_once_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_called_once()

    assert result == correct_result


def test_request_current_weather_03(mocker: pytest_mock.MockFixture):
    """
    Test forecast URL missing from first API call
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return ''
        def json(self):
            return {}

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    forecast_url_missing_message = provide_response_error_messages()[0]

    correct_result = CurrentWeather(
        valid_response = False,
        error_message = forecast_url_missing_message)

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_once_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_not_called()

    assert result == correct_result


def test_request_current_weather_04(mocker: pytest_mock.MockFixture):
    """
    Test invalid input coordinates
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_json = {'type': 'invalidPoint'}

    @dataclass
    class StationResponse:
        ok: bool = False
        url: str = ''
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    invalid_input_coordinates_message = provide_response_error_messages()[1]

    correct_result = CurrentWeather(
        valid_response = False,
        error_message = invalid_input_coordinates_message)

    coordinates = Coordinates(latitude=40, longitude=88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_not_called()

    assert result == correct_result


def test_request_current_weather_05(mocker: pytest_mock.MockFixture):
    """
    Test non-ok response from first API call but with valid input coordinates
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    @dataclass
    class StationResponse:
        ok: bool = False
        url: str = ''
        def text(self):
            return ''
        def json(self):
            return {'type': ''}

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    invalid_weather_api_response_message = provide_response_error_messages()[2]

    correct_result = CurrentWeather(
        valid_response = False,
        error_message = invalid_weather_api_response_message)

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_not_called()

    assert result == correct_result


def test_request_current_weather_06(mocker: pytest_mock.MockFixture):
    """
    Test temperature missing from second API call
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_url = 'https://api.weather.gov/points/000,000'
    forecast_url = 'https://api.weather.gov/gridpoints/XXX/000,000/forecast'
    station_json = {
        'properties': {
            'forecast': forecast_url,
            'relativeLocation': {
                'properties': {'city': 'Homer', 'state': 'IL'}},
            'radarStation': 'XXX'}}

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = station_url
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = forecast_url
        def text(self):
            return str(station_json)
        def json(self):
            return {'properties': {'periods': [{}]}}

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    invalid_weather_api_response_message = provide_response_error_messages()[3]

    correct_result = CurrentWeather(
        valid_response = True,
        error_message = invalid_weather_api_response_message)

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_once_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_called_once()

    assert result == correct_result


def test_request_current_weather_07(mocker: pytest_mock.MockFixture):
    """
    Test some location information ('city') missing from first API call
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_url = 'https://api.weather.gov/points/000,000'
    forecast_url = 'https://api.weather.gov/gridpoints/XXX/000,000/forecast'
    station_json = {
        'properties': {
            'forecast': forecast_url,
            'relativeLocation': {
                'properties': {'state': 'IL'}},
            'radarStation': 'XXX'}}

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = station_url
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = forecast_url
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    invalid_weather_api_response_message = provide_response_error_messages()[4]

    correct_result = CurrentWeather(
        valid_response = True,
        temperature_celsius = convert_fahrenheit_to_celsius(23),
        error_message = invalid_weather_api_response_message)

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_once_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_called_once()

    assert result == correct_result


def test_request_current_weather_08(mocker: pytest_mock.MockFixture):
    """
    Test non-ok response from second API call
    """


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_url = 'https://api.weather.gov/points/000,000'
    forecast_url = 'https://api.weather.gov/gridpoints/XXX/000,000/forecast'
    station_json = {
        'properties': {
            'forecast': forecast_url,
            'relativeLocation': {
                'properties': {'city': 'Homer', 'state': 'IL'}},
            'radarStation': 'XXX'}}

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = station_url
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = False
        url: str = forecast_url
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    correct_result = CurrentWeather(
        valid_response = False)

    coordinates = Coordinates(latitude=40, longitude=-88)
    result = request_current_weather(coordinates)

    station_mock.assert_called_once_with(
        (coordinates.latitude, coordinates.longitude))
    forecast_mock.assert_called()

    assert result == correct_result


@pytest.mark.skip(reason='calls external API; used to create mock response')
def test_get_historical_and_current_temperatures_01():
    """
    Test valid input and responses
    """

    user_submitted_coordinates = Coordinates(latitude=40, longitude=-88)
    station_coordinates = (40.053001403808594, -88.37300109863281)
    coordinates_distance = gd.distance(
        (user_submitted_coordinates.latitude, 
         user_submitted_coordinates.longitude), 
        station_coordinates).kilometers

    correct_result = AnnualWeather(
        current_temperature_celsius = convert_fahrenheit_to_celsius(23),
        current_station = 'KILX',
        current_city = 'Bondville',
        current_state = 'IL',
        current_error_message = '',
        distance_to_station_kilometers = coordinates_distance,
        annual_timestamp = [
            datetime.datetime(2019, 1, 1,  8, 0),
            datetime.datetime(2019, 1, 1, 16, 0),
            datetime.datetime(2020, 1, 1,  8, 0),
            datetime.datetime(2020, 1, 1, 16, 0)],
        annual_temperature_celsius = [-1.5, 0.0, 2.5, 10.0],
        annual_usaf_station_id = 999999,
        annual_wban_station_id = 54808,
        annual_station_name = 'CHAMPAIGN 9 SW',
        annual_station_state = 'IL',
        annual_station_call = '',
        annual_station_latitude = station_coordinates[0],
        annual_station_longitude = station_coordinates[1],
        annual_station_elevation_meters = 213.39999389648438)

    result = get_historical_and_current_temperatures(user_submitted_coordinates)

    # tests somewhat above highest- and below lowest-ever recorded temperatures
    assert (
        result.current_temperature_celsius > 
        convert_fahrenheit_to_celsius(-150))
    assert (
        result.current_temperature_celsius < 
        convert_fahrenheit_to_celsius(150))

    assert result.current_station == correct_result.current_station   
    assert result.current_city == correct_result.current_city
    assert result.current_state == correct_result.current_state
    assert result.current_error_message == correct_result.current_error_message
    assert (
        result.distance_to_station_kilometers == 
        correct_result.distance_to_station_kilometers)

    now = datetime.datetime.now()
    for e in result.annual_timestamp:
        isinstance(e, datetime.datetime)
        assert e > datetime.datetime(1979, 12, 31, 23, 59)
        assert e < now

    for e in result.annual_temperature_celsius:
        assert e > convert_fahrenheit_to_celsius(-150)
        assert e < convert_fahrenheit_to_celsius(150)

    assert result.annual_usaf_station_id == correct_result.annual_usaf_station_id
    assert result.annual_wban_station_id == correct_result.annual_wban_station_id
    assert result.annual_station_name == correct_result.annual_station_name
    assert result.annual_station_state == correct_result.annual_station_state
    assert result.annual_station_call == correct_result.annual_station_call
    assert (
        result.annual_station_latitude == 
        correct_result.annual_station_latitude)
    assert (
        result.annual_station_longitude == 
        correct_result.annual_station_longitude)
    assert (
        result.annual_station_elevation_meters == 
        correct_result.annual_station_elevation_meters)


def test_get_historical_and_current_temperatures_02(
    mocker: pytest_mock.MockFixture):
    """
    Test valid input and responses
    """


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_station_df(dummy):
        return pl.DataFrame({
            'usaf': [999999],
            'wban': [54808],
            'station_name': ['CHAMPAIGN 9 SW'],
            'st': ['IL'],
            'call': ['XXX'],
            'lat': [40.053001],
            'lon': [-88.373001],
            'elev(m)': [213.399994]})

    mocker.patch('src.step04_api.api.get_station_df', mock_get_station_df)


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_historical_temperatures(dummy1, dummy2):
        return pl.DataFrame({
            'timestamp': [
                datetime.datetime(2019, 1, 1,  8, 0),
                datetime.datetime(2019, 1, 1, 16, 0),
                datetime.datetime(2020, 1, 1,  8, 0),
                datetime.datetime(2020, 1, 1, 16, 0)],
            'temperature': [-15, 0, 25, 100]})

    mocker.patch(
        'src.step04_api.api.get_historical_temperatures', 
        mock_get_historical_temperatures)


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_url = 'https://api.weather.gov/points/000,000'
    forecast_url = 'https://api.weather.gov/gridpoints/XXX/000,000/forecast'
    station_json = {
        'properties': {
            'forecast': forecast_url,
            'relativeLocation': {
                'properties': {'city': 'Homer', 'state': 'IL'}},
            'radarStation': 'XXX'}}

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = station_url
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = forecast_url
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    user_submitted_coordinates = Coordinates(latitude=40, longitude=-88)
    station_coordinates = (
        mock_get_station_df(0)['lat'][0], mock_get_station_df(0)['lon'][0])
    coordinates_distance = gd.distance(
        (user_submitted_coordinates.latitude, 
         user_submitted_coordinates.longitude), 
        station_coordinates).kilometers

    correct_result = AnnualWeather(
        current_temperature_celsius = convert_fahrenheit_to_celsius(23),
        current_station = 'XXX',
        current_city = 'Homer',
        current_state = 'IL',
        current_error_message = '',
        distance_to_station_kilometers = coordinates_distance,
        annual_timestamp = [
            datetime.datetime(2019, 1, 1,  8, 0),
            datetime.datetime(2019, 1, 1, 16, 0),
            datetime.datetime(2020, 1, 1,  8, 0),
            datetime.datetime(2020, 1, 1, 16, 0)],
        annual_temperature_celsius = [-1.5, 0.0, 2.5, 10.0],
        annual_usaf_station_id = 999999,
        annual_wban_station_id = 54808,
        annual_station_name = 'CHAMPAIGN 9 SW',
        annual_station_state = 'IL',
        annual_station_call = 'XXX',
        annual_station_latitude = 40.053001,
        annual_station_longitude = -88.373001,
        annual_station_elevation_meters = 213.399994)

    result = get_historical_and_current_temperatures(user_submitted_coordinates)

    station_mock.assert_called_once_with(
        (station_coordinates[0], station_coordinates[1]))
    forecast_mock.assert_called_once()

    assert result == correct_result


def test_get_historical_and_current_temperatures_03(
    mocker: pytest_mock.MockFixture):
    """
    Test one error message derived from calling 'request_current_weather' that
        returns an instance of 'CurrentWeather':

        Test forecast URL missing from first API call

    All error messages for 'request_current_weather' are tested specifically
        for that function, so not all of them are tested for this function
        'get_historical_and_current_temperatures'
    """


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_station_df(dummy):
        return pl.DataFrame({
            'usaf': [999999],
            'wban': [54808],
            'station_name': ['CHAMPAIGN 9 SW'],
            'st': ['IL'],
            'call': ['XXX'],
            'lat': [40.053001],
            'lon': [-88.373001],
            'elev(m)': [213.399994]})

    mocker.patch('src.step04_api.api.get_station_df', mock_get_station_df)


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_historical_temperatures(dummy1, dummy2):
        return pl.DataFrame({
            'timestamp': [
                datetime.datetime(2019, 1, 1,  8, 0),
                datetime.datetime(2019, 1, 1, 16, 0),
                datetime.datetime(2020, 1, 1,  8, 0),
                datetime.datetime(2020, 1, 1, 16, 0)],
            'temperature': [-15, 0, 25, 100]})

    mocker.patch(
        'src.step04_api.api.get_historical_temperatures', 
        mock_get_historical_temperatures)


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    @dataclass
    class StationResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return ''
        def json(self):
            return {}

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    forecast_url_missing_message = provide_response_error_messages()[0]

    user_submitted_coordinates = Coordinates(latitude=40, longitude=-88)
    station_coordinates = (
        mock_get_station_df(0)['lat'][0], mock_get_station_df(0)['lon'][0])
    coordinates_distance = gd.distance(
        (user_submitted_coordinates.latitude, 
         user_submitted_coordinates.longitude), 
        station_coordinates).kilometers

    correct_result = AnnualWeather(
        current_temperature_celsius = -9999,
        current_station = '',
        current_city = '',
        current_state = '',
        current_error_message = forecast_url_missing_message,
        distance_to_station_kilometers = coordinates_distance,
        annual_timestamp = [
            datetime.datetime(2019, 1, 1,  8, 0),
            datetime.datetime(2019, 1, 1, 16, 0),
            datetime.datetime(2020, 1, 1,  8, 0),
            datetime.datetime(2020, 1, 1, 16, 0)],
        annual_temperature_celsius = [-1.5, 0.0, 2.5, 10.0],
        annual_usaf_station_id = 999999,
        annual_wban_station_id = 54808,
        annual_station_name = 'CHAMPAIGN 9 SW',
        annual_station_state = 'IL',
        annual_station_call = 'XXX',
        annual_station_latitude = 40.053001,
        annual_station_longitude = -88.373001,
        annual_station_elevation_meters = 213.399994)

    result = get_historical_and_current_temperatures(user_submitted_coordinates)

    station_mock.assert_called_once_with(
        (station_coordinates[0], station_coordinates[1]))
    forecast_mock.assert_not_called()

    assert result == correct_result


def test_get_historical_and_current_temperatures_04(
    mocker: pytest_mock.MockFixture):
    """
    Test one error message derived from calling 'request_current_weather' that
        returns an instance of 'CurrentWeather':

        Test invalid input coordinates

    All error messages for 'request_current_weather' are tested specifically
        for that function, so not all of them are tested for this function
        'get_historical_and_current_temperatures'
    """


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_station_df(dummy):
        return pl.DataFrame({
            'usaf': [999999],
            'wban': [54808],
            'station_name': ['CHAMPAIGN 9 SW'],
            'st': ['IL'],
            'call': ['XXX'],
            'lat': [40.053001],
            'lon': [-88.373001],
            'elev(m)': [213.399994]})

    mocker.patch('src.step04_api.api.get_station_df', mock_get_station_df)


    # SET UP MOCK FOR RETRIEVING TABLE OF WEATHER STATION INFORMATION
    ##################################################

    def mock_get_historical_temperatures(dummy1, dummy2):
        return pl.DataFrame({
            'timestamp': [
                datetime.datetime(2019, 1, 1,  8, 0),
                datetime.datetime(2019, 1, 1, 16, 0),
                datetime.datetime(2020, 1, 1,  8, 0),
                datetime.datetime(2020, 1, 1, 16, 0)],
            'temperature': [-15, 0, 25, 100]})

    mocker.patch(
        'src.step04_api.api.get_historical_temperatures', 
        mock_get_historical_temperatures)


    # SET UP MOCK FOR FIRST API CALL, WHICH REQUESTS STATION INFORMATION
    ##################################################

    station_json = {'type': 'invalidPoint'}

    @dataclass
    class StationResponse:
        ok: bool = False
        url: str = ''
        def text(self):
            return str(station_json)
        def json(self):
            return station_json 

    def mock_request_station_information(dummy):
        station_response = StationResponse()
        return station_response

    station_mock = mocker.Mock(wraps=mock_request_station_information)
    mocker.patch('src.step04_api.api.request_station_information', station_mock)
    mocker.patch('src.step04_api.api.sleep', mock_sleep)


    # SET UP MOCK FOR SECOND API CALL, WHICH REQUESTS FORECAST
    ##################################################

    forecast_dict = {'properties': {'periods': [{'temperature': 23}]}}

    @dataclass
    class ForecastResponse:
        ok: bool = True
        url: str = ''
        def text(self):
            return str(forecast_dict)
        def json(self):
            return forecast_dict

    def mock_request_forecast(dummy):
        forecast_response = ForecastResponse()
        return forecast_response 

    forecast_mock = mocker.Mock(wraps=mock_request_forecast)
    mocker.patch('src.step04_api.api.requests.get', forecast_mock)


    # RUN TEST
    ##################################################

    invalid_input_coordinates_message = provide_response_error_messages()[1]

    user_submitted_coordinates = Coordinates(latitude=40, longitude=88)
    station_coordinates = (
        mock_get_station_df(0)['lat'][0], mock_get_station_df(0)['lon'][0])
    coordinates_distance = gd.distance(
        (user_submitted_coordinates.latitude, 
         user_submitted_coordinates.longitude), 
        station_coordinates).kilometers

    correct_result = AnnualWeather(
        current_temperature_celsius = -9999,
        current_station = '',
        current_city = '',
        current_state = '',
        current_error_message = invalid_input_coordinates_message,
        distance_to_station_kilometers = coordinates_distance,
        annual_timestamp = [
            datetime.datetime(2019, 1, 1,  8, 0),
            datetime.datetime(2019, 1, 1, 16, 0),
            datetime.datetime(2020, 1, 1,  8, 0),
            datetime.datetime(2020, 1, 1, 16, 0)],
        annual_temperature_celsius = [-1.5, 0.0, 2.5, 10.0],
        annual_usaf_station_id = 999999,
        annual_wban_station_id = 54808,
        annual_station_name = 'CHAMPAIGN 9 SW',
        annual_station_state = 'IL',
        annual_station_call = 'XXX',
        annual_station_latitude = 40.053001,
        annual_station_longitude = -88.373001,
        annual_station_elevation_meters = 213.399994)

    result = get_historical_and_current_temperatures(user_submitted_coordinates)

    station_mock.assert_called_with(
        (station_coordinates[0], station_coordinates[1]))
    forecast_mock.assert_not_called()

    assert result == correct_result



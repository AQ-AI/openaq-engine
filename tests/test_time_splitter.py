import datetime
import json

from src.time_splitter import TimeSplitter
from src.utils.utils import query_results_from_api


def test_get_end_time_windows():
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    window_start_date = datetime.date(2020, 1, 1)

    # Call the method and get the end date
    end_date = time_splitter._get_end_time_windows(window_start_date)

    # Assert the expected end date is returned
    assert end_date == datetime.date(2020, 3, 1)


def test_get_start_time_windows():
    """Tests the _get_start_time_windows method"""
    # Mock the relativedelta method to return expected start dates

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    end_date = datetime.date(2020, 12, 1)

    # Call the method twice and assert the expected start dates are returned
    start_date = time_splitter._get_start_time_windows(end_date, 0)
    assert isinstance(start_date, datetime.date)

    start_date = time_splitter._get_start_time_windows(end_date, 1)
    assert isinstance(start_date, datetime.date)


def test_get_validation_window(mocker):
    """Tests the get_validation_window method"""
    mocker.patch("src.time_splitter.mlflow")
    # Mock the _get_start_time_windows and _get_end_time_windows methods
    mocker.patch.object(
        TimeSplitter,
        "_get_start_time_windows",
        return_value=datetime.date(2020, 1, 1),
    )
    mocker.patch.object(
        TimeSplitter,
        "_get_end_time_windows",
        return_value=datetime.date(2020, 3, 1),
    )

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    end_date = datetime.date(2020, 6, 1)
    window_no = 2

    # Call the method and assert the expected start and end dates are returned
    start_date, end_date = time_splitter.get_validation_window(
        end_date, window_no
    )
    assert start_date == datetime.date(2020, 1, 1)
    assert end_date == datetime.date(2020, 3, 1)


def test_execute_for_openaq_aws(mocker):
    params = {"mocked": "params"}
    city = "London"
    country = "UK"
    pollutant = "pm25"
    latest_date = "2020-01-01"
    # Mock the calls to create_end_date_from_aws and create_start_date_from_aws
    mocker.patch.object(
        TimeSplitter,
        "create_end_date_from_aws",
        return_value=datetime.date(2020, 1, 1),
    )
    mocker.patch.object(
        TimeSplitter,
        "create_start_date_from_aws",
        return_value=datetime.date(2019, 6, 1),
    )

    # Instantiate TimeSplitter
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )

    # Call execute_for_openaq_aws and get results
    end_date, start_date = time_splitter.execute_for_openaq_aws(
        params, city, country, pollutant, latest_date
    )

    # Assert expected end_date and start_date are returned from the mocked methods
    assert end_date == datetime.date(2020, 1, 1)
    assert start_date == datetime.date(2019, 6, 1)


def test_create_start_date_from_aws(mocker):
    # Mock the required arguments
    params = {"region": "us-east-1"}
    city = "London"
    country_info = "UK"
    pollutant = "pm25"
    latest_date = "2020-01-01"

    # Mock the build_response_from_aws method to return a start date
    mocker.patch.object(
        TimeSplitter,
        "build_response_from_aws",
        return_value="2020-01-01 00:00:00.000 UTC",
    )

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable=pollutant,
        country=country_info,
        source="openaq-aws",
    )

    # Call the method and get the start date
    start_date = time_splitter.create_start_date_from_aws(
        params, city, country_info, pollutant, latest_date
    )

    # Assert the expected start date is returned
    assert start_date == datetime.date(2020, 1, 1)

    # Assert build_response_from_aws was called with the expected SQL query
    sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            AND city='{city}'
            ORDER BY {date_col} ASC limit 1;""".format(
        table=time_splitter.table_name,
        date_col=time_splitter.date_col,
        target_variable=pollutant,
        city=city,
        latest_date=latest_date,
    )
    time_splitter.build_response_from_aws.assert_called_with(params, sql_query)

    city = ""
    # Call the method and get the start date (country)
    start_date = time_splitter.create_start_date_from_aws(
        params, city, country_info, pollutant, latest_date
    )

    # Assert build_response_from_aws was called with the expected SQL query (country)
    sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            AND country='{country}'
            ORDER BY {date_col} ASC limit 1;""".format(
        table=time_splitter.table_name,
        date_col=time_splitter.date_col,
        target_variable=pollutant,
        country=country_info,
        latest_date=latest_date,
    )
    time_splitter.build_response_from_aws.assert_called_with(params, sql_query)

    country_info = "WO"
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable=pollutant,
        country=country_info,
        source="openaq-aws",
    )

    # Call the method and get the start date (country)
    start_date = time_splitter.create_start_date_from_aws(
        params, city, country_info, pollutant, latest_date
    )

    # Assert build_response_from_aws was called with the expected SQL query (country)
    sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            AND country='{country}'
            ORDER BY {date_col} ASC limit 1;""".format(
        table=time_splitter.table_name,
        date_col=time_splitter.date_col,
        target_variable=pollutant,
        country=country_info,
        latest_date=latest_date,
    )
    time_splitter.build_response_from_aws.assert_called_with(params, sql_query)


def test_create_end_date_from_openaq_api(mocker):
    # Mock the required arguments
    city = "London"
    country = ""
    sensor_type = "reference grade"
    pollutant = "pm25"

    mock_response = {"results": [{"lastUpdated": "2020-01-01T00:00:00+00:00"}]}

    mock_query_results_from_api = mocker.MagicMock(
        name="src.utils.utils.query_results_from_api"
    )
    mocker.patch(
        "src.utils.utils.query_results_from_api",
        mock_query_results_from_api,
        return_value=json.dumps(mock_response),
    )

    # # Mock the query_results_from_api method to return mock API response
    # mocker.patch(
    #     "src.utils.utils.query_results_from_api",
    #     return_value=json.dumps(mock_response),
    # )

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-api",
    )

    # Call the method and get the end date
    end_date = time_splitter.create_end_date_from_openaq_api(
        city, country, sensor_type, pollutant
    )

    # Assert the expected end date is returned
    assert end_date == datetime.date(2023, 7, 5)

    # Assert query_results_from_api was called with the expected URL
    expected_url = f"""https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=100&country={country}&order_by=lastUpdated&sensorType={sensor_type}&dumpRaw=false""".format(
        pollutant=pollutant, country=country, sensor_type=sensor_type
    )
    mock_query_results_from_api.assert_called_with(
        {"accept": "application/json"}, expected_url
    )
    query_results_from_api.mock.assert_called_with(
        {"accept": "application/json"}, expected_url
    )

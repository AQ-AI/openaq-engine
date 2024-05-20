import datetime
import json
from unittest.mock import patch

import pandas as pd

from src.time_splitter import TimeSplitter


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
    city = "Mumbai"
    country = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"
    local_data = "cohorts_Mumbai"

    # Mock get_data to return a DataFrame
    mocker.patch(
        "openaq_engine.src.utils.utils.get_data", return_value=pd.DataFrame()
    )

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
        train_validation_dict={"training": [], "validation": []},
        target_variable="pm25",
        country="UK",
        source="openaq-aws",
    )

    # Call execute and get results
    with patch("src.utils.utils.get_data", return_value=pd.DataFrame()):
        results = time_splitter.execute(
            city,
            country,
            None,
            "openaq-aws",
            pollutant,
            latest_date,
            local_data,
        )

    # Check the expected values in results
    assert results is not None
    assert "validation" in results
    assert "training" in results


def test_create_start_date_from_aws(mocker):
    # Mock the required arguments
    params = {"region": "us-east-1"}
    city = "Mumbai"
    country_info = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"

    # Mock get_data to return a DataFrame
    mocker.patch("src.utils.utils.get_data", return_value=pd.DataFrame())

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
        params, city, country_info, pollutant, latest_date, pd.DataFrame()
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
        params, city, country_info, pollutant, latest_date, pd.DataFrame()
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
        params, city, country_info, pollutant, latest_date, pd.DataFrame()
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
    city = "Baraboo"
    country = "US"
    sensor_type = "reference grade"
    pollutant = "pm25"

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-api",
    )

    # Mock the API response
    mock_response = {"results": [{"lastUpdated": "2023-04-01T21:00:00+00:00"}]}
    mocker.patch(
        "src.utils.utils.query_results_from_api",
        return_value=type(
            "obj", (object,), {"text": json.dumps(mock_response)}
        ),
    )

    # Call the method and get the end date
    end_date = time_splitter.create_end_date_from_openaq_api(
        city, country, sensor_type, pollutant, pd.DataFrame()
    )

    assert end_date == datetime.datetime.now().date()

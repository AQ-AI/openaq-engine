import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.engine import Engine

from openaq_engine.src.time_splitter import TimeSplitter, TimeSplitterBase


@pytest.fixture
def mock_db_connection():
    mock_engine = MagicMock(spec=Engine)
    mock_connection = MagicMock()
    mock_engine.connect.return_value = mock_connection
    with patch("setup_environment.get_dbengine", return_value=mock_engine):
        with patch(
            "setup_environment.connect_to_db", return_value=mock_connection
        ):
            yield mock_connection


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
    end_date = time_splitter._get_end_time_windows(window_start_date)
    assert end_date == datetime.date(2020, 3, 1)


def test_get_start_time_windows():
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
    start_date = time_splitter._get_start_time_windows(end_date, 0)
    assert isinstance(start_date, datetime.date)
    start_date = time_splitter._get_start_time_windows(end_date, 1)
    assert isinstance(start_date, datetime.date)


def test_get_validation_window(mocker):
    mocker.patch("openaq_engine.src.time_splitter.mlflow")
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
    start_date, end_date = time_splitter.get_validation_window(
        end_date, window_no
    )
    assert start_date == datetime.date(2020, 1, 1)
    assert end_date == datetime.date(2020, 3, 1)


def test_create_start_date_from_aws(mocker):
    params = {
        "region": "us-east-1",
        "database": "test_db",
        "bucket": "testbucket",
        "path": "test_path",
    }
    country_info = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"

    # Mock the Athena client and its methods
    mock_athena_client = MagicMock()
    mock_athena_client.start_query_execution.return_value = {
        "QueryExecutionId": "12345"
    }
    mock_athena_client.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "Header"}]},
                {"Data": [{"VarCharValue": "2021-01-01 00:00:00.000 UTC"}]},
            ]
        }
    }

    # Patch boto3 client creation to return the mock client
    with patch("boto3.Session.client", return_value=mock_athena_client):
        time_splitter = TimeSplitter(
            time_window_length=6,
            within_window_sampler=2,
            window_count=3,
            train_validation_dict={},
            target_variable=pollutant,
            country=country_info,
            source="openaq-aws",
        )
        start_date = time_splitter.create_start_date_from_aws(
            params, country_info, pollutant, latest_date
        )
        assert start_date == datetime.date(2021, 1, 1)


def test_build_response_from_aws(mocker):
    params = {
        "region": "us-east-1",
        "database": "test_db",
        "bucket": "testbucket",
        "path": "test_path",
    }
    sql_query = "SELECT * FROM test_table"

    # Mock the Athena client and its methods
    mock_athena_client = MagicMock()
    mock_athena_client.start_query_execution.return_value = {
        "QueryExecutionId": "12345"
    }
    mock_athena_client.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "Header"}]},
                {"Data": [{"VarCharValue": "2021-01-01 00:00:00.000 UTC"}]},
            ]
        }
    }

    # Patch boto3 client creation to return the mock client
    with patch("boto3.Session.client", return_value=mock_athena_client):
        time_splitter_base = TimeSplitterBase(
            "date",
            "test_table",
            "test_db",
            "us-east-1",
            "testbucket",
            "test_output",
        )
        response = time_splitter_base.build_response_from_aws(
            params, sql_query
        )
        assert response == "2021-01-01 00:00:00.000 UTC"


def test_create_end_date_from_openaq_api(mocker):
    # Mock the required arguments
    country = "US"
    pollutant = "pm25"
    latest_date = "2021-12-23"

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-api",
    )

    # Create a mock response object
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": [
            {
                "date": {
                    "utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                }
            }
        ]
    }

    # Mock the API call
    mocker.patch("requests.get", return_value=mock_response)

    # Call the method and get the end date
    end_date = time_splitter.create_end_date_from_openaq_api(
        country,
        pollutant,
        latest_date,
    )

    # Assertions
    assert end_date == datetime.utcnow().date()


def test_create_start_date_from_openaq_api(mocker):
    # Mock the required arguments
    country = "US"
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
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "results": [{"firstUpdated": "2023-04-01T21:00:00+00:00"}]
    }

    mocker.patch(
        "openaq_engine.src.utils.utils.query_results_from_api",
        return_value=mock_response,
    )

    # Call the method and get the start date
    start_date = time_splitter.create_start_date_from_openaq_api(
        country,
        pollutant,
    )

    # Assertions
    assert start_date == datetime.date(2023, 4, 1)

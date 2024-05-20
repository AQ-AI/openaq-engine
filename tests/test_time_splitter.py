import datetime
import json
from unittest.mock import MagicMock, patch

import mlflow
import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from src.time_splitter import TimeSplitter, TimeSplitterBase


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
    mocker.patch("src.time_splitter.mlflow")
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


def test_create_end_date_from_aws(mocker):
    params = {
        "region": "us-east-1",
        "database": "test_db",
        "bucket": "testbucket",
        "path": "test_path",
    }
    city = "Mumbai"
    country_info = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"
    local_df = pd.DataFrame()

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
                {"Data": [{"VarCharValue": "2021-12-23 00:00:00.000 UTC"}]},
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
        end_date = time_splitter.create_end_date_from_aws(
            params, city, country_info, pollutant, latest_date, local_df
        )
        assert end_date == datetime.date(2021, 12, 23)


def test_create_start_date_from_aws(mocker):
    params = {
        "region": "us-east-1",
        "database": "test_db",
        "bucket": "testbucket",
        "path": "test_path",
    }
    city = "Mumbai"
    country_info = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"
    local_df = pd.DataFrame()

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
            params, city, country_info, pollutant, latest_date, local_df
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


def test_create_start_date_from_openaq_api(mocker):
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
    mock_response = {
        "results": [{"firstUpdated": "2023-04-01T21:00:00+00:00"}]
    }
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


def test_create_start_local_data():
    local_df = pd.DataFrame(
        {
            "date": [
                '{"utc": "2023-03-31T23:30:00+00:00", "local": "2023-03-31T23:30:00+05:30"}'
            ]
        }
    )
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="IN",
        source="openaq-api",
    )
    start_date = time_splitter.create_start_local_data(local_df)
    assert start_date == datetime.date(2023, 3, 31)


def test_create_end_local_data():
    local_df = pd.DataFrame(
        {
            "date": [
                '{"utc": "2023-03-31T23:30:00+00:00", "local": "2023-03-31T23:30:00+05:30"}'
            ]
        }
    )
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="IN",
        source="openaq-api",
    )
    end_date = time_splitter.create_end_local_data(local_df)
    assert end_date == datetime.date(2023, 3, 31)


def test_execute_for_openaq_aws(mocker, mock_db_connection):
    city = "Mumbai"
    country = "IN"
    pollutant = "pm25"
    latest_date = "2021-12-23"
    local_data = "cohorts_Mumbai"
    mocker.patch("src.utils.utils.get_data", return_value=pd.DataFrame())
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
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={"training": [], "validation": []},
        target_variable="pm25",
        country="UK",
        source="openaq-aws",
    )
    results = time_splitter.execute(
        city, country, None, "openaq-aws", pollutant, latest_date, local_data
    )
    assert results is not None
    assert "validation" in results
    assert "training" in results


def test_execute_for_openaq_api(mocker, mock_db_connection):
    city = "Mumbai"
    country = "IN"
    pollutant = "pm25"
    sensor_type = "reference grade"
    latest_date = "2021-12-23"
    local_data = "cohorts_Mumbai"

    mocker.patch("src.utils.utils.get_data", return_value=pd.DataFrame())
    mocker.patch.object(
        TimeSplitter,
        "create_end_date_from_openaq_api",
        return_value=datetime.date(2020, 1, 1),
    )
    mocker.patch.object(
        TimeSplitter,
        "create_start_date_from_openaq_api",
        return_value=datetime.date(2019, 6, 1),
    )

    mock_mlflow = mocker.patch("mlflow.start_run")
    mock_mlflow_run = MagicMock()
    mock_mlflow.return_value = mock_mlflow_run

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={"training": [], "validation": []},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )

    mocker.patch(
        "mlflow.log_param"
    )  # Mock mlflow.log_param to avoid conflicts

    results = time_splitter.execute(
        city,
        country,
        sensor_type,
        "openaq-api",
        pollutant,
        latest_date,
        local_data,
    )

    assert results is not None
    assert "validation" in results
    assert "training" in results

    # Check that mlflow.log_param is called with correct arguments
    mlflow.log_param.assert_any_call("source", "openaq-api")


def test_format_to_date_only():
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="IN",
        source="openaq-api",
    )
    dt = datetime.datetime(2023, 5, 20)
    formatted_date = time_splitter.format_to_date_only(dt)
    assert formatted_date == "2023-05-20"

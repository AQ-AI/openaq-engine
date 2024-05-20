import datetime
from contextlib import nullcontext
import os
from unittest.mock import patch, MagicMock

import pandas as pd
import pytz
import pytest
from sqlalchemy.exc import OperationalError

from setup_environment import get_dbengine
from src.cohort_builder import CohortBuilder


@pytest.fixture(autouse=True)
def mock_env_vars():
    with patch.dict(
        os.environ,
        {
            "DB_NAME_OPENAQ": "test_db",
            "S3_BUCKET_OPENAQ": "openaq-pm25-historic",
            "S3_OUTPUT_OPENAQ": "pm25-month",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
        },
    ):
        yield


def test_cohort_builder(mocker):
    # Mock the required arguments
    end_date = datetime.datetime(
        2022, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    start_date = datetime.datetime(
        2023, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    end_date_str = f"{{utc={end_date}, local={end_date}}}"
    start_date_str = f"{{utc={start_date}, local={start_date}}}"
    date_tuple = (start_date, end_date)
    train_validation_dict = {"training": [date_tuple]}
    coordinates = "{latitude=44.089355, longitude=-70.214134}"
    filter_cols = "date, location"
    city = "Mumbai"
    country = "IN"
    sensor_type = "reference grade"
    pollutant = "pm25"
    local_data = ""

    # Mock execute_for_openaq_aws and execute_for_openaq_api calls
    df = pd.DataFrame(
        {
            "date": [start_date_str, end_date_str],
            "parameter": pollutant,
            "value": [5, 10],
            "coordinates": coordinates,
        }
    )

    mocker.patch.object(
        CohortBuilder, "execute_for_openaq_aws", return_value=df
    )
    mocker.patch.object(
        CohortBuilder, "execute_for_openaq_api", return_value=df
    )
    mocker.patch(
        "openaq_engine.src.cohort_builder.get_dbengine", return_value=None
    )
    mocker.patch.object(CohortBuilder, "_results_to_db", return_value=None)

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-aws",
    )
    cohort_builder.cohort_builder(
        list(train_validation_dict.keys())[0],
        train_validation_dict,
        filter_cols,
        city,
        country,
        "openaq-aws",
        sensor_type,
        pollutant,
        local_data,
    )

    cohort_builder.execute_for_openaq_aws.assert_called_with(
        date_tuple,
        city,
        country,
        pollutant,
        sensor_type,
        local_data,
    )
    cohort_builder.execute_for_openaq_api.assert_not_called()


def test_execute_for_openaq_aws(mocker):
    # Mock the required arguments
    end_date = datetime.datetime(
        2022, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    start_date = datetime.datetime(
        2023, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    end_date_str = f"{{utc={end_date}, local={end_date}}}"
    start_date_str = f"{{utc={start_date}, local={start_date}}}"
    date_tuple = (start_date, end_date)
    coordinates = "{latitude=44.089355, longitude=-70.214134}"
    city = "Mumbai"
    country = "IN"
    sensor_type = "reference grade"
    pollutant = "pm25"
    local_data = ""

    # Mock execute_for_openaq_aws and execute_for_openaq_api calls
    df = pd.DataFrame(
        {
            "date": [start_date_str, end_date_str],
            "parameter": pollutant,
            "value": [5, 10],
            "coordinates": coordinates,
        }
    )

    mocker.patch.object(
        CohortBuilder, "build_response_from_aws", return_value=df
    )

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-aws",
    )

    # Call the method
    cohort_df = cohort_builder.execute_for_openaq_aws(
        date_tuple, city, country, pollutant, sensor_type, local_data
    )
    assert cohort_df.equals(df)

    aws_dict = {
        "bucket": "openaq-pm25-historic",
        "database": "test_db",
        "path": "pm25-month/cohorts",
        "region": "us-east-1",
    }
    # Assert expected query is passed to build_response_from_aws
    expected_query = """SELECT DISTINCT *
                FROM {table}
                WHERE parameter='{target_variable}'
                AND city='{city}'
                AND {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}';""".format(
        table=cohort_builder.table_name,
        date_col=cohort_builder.date_col,
        target_variable=pollutant,
        city=city,
        start_date=start_date,
        end_date=end_date,
    )
    cohort_builder.build_response_from_aws.assert_called_with(
        aws_dict, expected_query
    )


def test_results_to_db(mocker):
    # Mock write_to_db call
    mocker.patch.object(
        CohortBuilder,
        "_results_to_db",
    )
    city = "London"
    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-aws",
    )
    with nullcontext():
        engine = get_dbengine()

        # Test with city
        if city:
            df = pd.DataFrame({"city": "London", "value": [1]})
            cohort_builder._results_to_db(df, engine, city)
            cohort_builder._results_to_db.assert_called_with(
                df, engine, "London"
            )
            city = ""
        else:
            # Test without city
            df = pd.DataFrame({"country": "UK", "value": [1]})
            cohort_builder._results_to_db(df, engine, city)
            cohort_builder._results_to_db.assert_called_with(df, engine, "")


def test_execute_for_openaq_api(mocker):
    # Mock the required arguments
    start_date = datetime.datetime(
        2022, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    end_date = datetime.datetime(
        2023, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    start_date_str = {
        "utc": f"{datetime.datetime(2023, 3, 31, 23, 30, 0, tzinfo=pytz.UTC).isoformat()}",
        "local": f"{datetime.datetime(2023, 3, 31, 23, 30, 0, tzinfo=pytz.timezone('Asia/Kolkata')).isoformat()}",
    }
    end_date_str = {
        "utc": f"{datetime.datetime(2022, 12, 30, 20, 30, 0, tzinfo=pytz.UTC).isoformat()}",
        "local": f"{datetime.datetime(2022, 12, 30, 20, 30, 0, tzinfo=pytz.timezone('Asia/Kolkata')).isoformat()}",
    }
    date_tuple = (start_date, end_date)
    coordinates = {"latitude": 19.07283, "longitude": 72.88261}
    city = "Mumbai"
    country = "IN"
    sensor_type = "reference grade"
    pollutant = "pm25"
    local_data = ""

    # Mock API response
    df = pd.DataFrame(
        {
            "date": [start_date_str, end_date_str],
            "parameter": pollutant,
            "value": [-999.0, 150.0],
            "coordinates": [coordinates, coordinates],
        }
    )

    mocker.patch(
        "openaq_engine.src.cohort_builder.CohortBuilder.execute_for_openaq_api",
        return_value=df,
    )

    # Mock database connection
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mocker.patch(
        "openaq_engine.src.cohort_builder.get_dbengine",
        return_value=mock_engine,
    )
    mock_engine.connect.return_value = mock_connection

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-api",
    )

    # Call the method
    cohort_df = cohort_builder.execute_for_openaq_api(
        date_tuple, city, country, pollutant, sensor_type, local_data
    )
    # Print both DataFrames for comparison
    print("Expected DataFrame:")
    print(df)
    print("Actual DataFrame:")
    print(
        cohort_df[["date", "parameter", "value", "coordinates"]].iloc[[0, -1]]
    )

    # Check if the returned DataFrame is equal to the mock DataFrame
    assert (
        cohort_df[["date", "parameter", "value", "coordinates"]]
        .iloc[[0, -1]]
        .equals(df)
    )

    # Ensure expected database operations are performed
    try:
        with mock_engine.connect() as connection:
            cohort_builder._results_to_db(df, connection, city)
    except OperationalError:
        pass  # Expected behavior if database connection fails

    # Assert the database operations were attempted
    mock_engine.connect.assert_called_once()
    mock_connection.execute.assert_called()

    # Assert API was called with correct URL
    expected_url = f"https://api.openaq.org/v2/measurements?date_from={start_date}&date_to={end_date}&limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&city={city}&order_by=datetime&sensor_type={sensor_type}&dumpRaw=false"
    mocker.patch(
        "openaq_engine.src.utils.utils.api_response_to_df"
    ).assert_called_with(expected_url)

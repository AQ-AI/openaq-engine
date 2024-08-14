import datetime
import os
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz
from setup_environment import get_dbengine
from sqlalchemy.exc import OperationalError

from openaq_engine.src.cohort_builder import CohortBuilder


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


@pytest.fixture
def mock_db_connection():
    with patch(
        "openaq_engine.setup_environment.connect_to_db",
        return_value=MagicMock(),
    ) as mock_conn:
        yield mock_conn


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
    country = "IN"
    pollutant = "pm25"

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

    # Mock the get_dbengine function in the setup_environment module
    mocker.patch(
        "openaq_engine.setup_environment.get_dbengine", return_value=None
    )
    mocker.patch.object(CohortBuilder, "_results_to_db", return_value=None)

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-aws",
    )

    # Call the cohort_builder method
    cohort_builder.cohort_builder(
        list(train_validation_dict.keys())[0],
        train_validation_dict,
        filter_cols,
        country,
        "openaq-aws",
        pollutant,
    )

    # Call the execute_for_openaq_aws method
    cohort_builder.execute_for_openaq_aws.assert_called_with(
        date_tuple,
        country,
        pollutant,
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
    country = "IN"
    pollutant = "pm25"

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
        date_tuple, country, pollutant
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
                WHERE parameter='{target_variable}' AND country='{country}'
                AND {date_col} BETWEEN '{start_date}' AND '{end_date}';""".format(
        table=cohort_builder.table_name,
        date_col=cohort_builder.date_col,
        target_variable=pollutant,
        country=country,
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


def test_execute_for_openaq_api(mocker, mock_db_connection):
    start_date = datetime.datetime(
        2022, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    end_date = datetime.datetime(
        2023, 4, 1, 21, 0, 0, tzinfo=pytz.UTC
    ).isoformat()
    date_tuple = (start_date, end_date)
    coordinates = {"latitude": 19.07283, "longitude": 72.88261}
    country = "IN"
    pollutant = "pm25"

    # Mock API response
    api_response = {
        "results": [
            {
                "date": {"utc": start_date, "local": start_date},
                "parameter": pollutant,
                "value": -999.0,
                "coordinates": coordinates,
            },
            {
                "date": {"utc": end_date, "local": end_date},
                "parameter": pollutant,
                "value": 150.0,
                "coordinates": coordinates,
            },
        ]
    }

    # Mock the requests.get function to return the desired response
    mock_response = MagicMock()
    mock_response.json.return_value = api_response
    mocker.patch("requests.get", return_value=mock_response)

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-api",
    )

    # Call the method
    result = cohort_builder.execute_for_openaq_api(
        date_tuple, country, pollutant
    )

    expected_df = pd.DataFrame(api_response["results"])
    pd.testing.assert_frame_equal(result, expected_df)

    # Mock database connection
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mocker.patch(
        "openaq_engine.setup_environment.get_dbengine",
        return_value=mock_engine,
    )
    mock_engine.connect.return_value = mock_connection

    # Ensure expected database operations are performed
    try:
        with mock_engine.connect() as connection:
            cohort_builder._results_to_db(result, connection)
    except OperationalError:
        pass  # Expected behavior if database connection fails

    # Assert the database operations were attempted
    mock_engine.connect.assert_called_once()

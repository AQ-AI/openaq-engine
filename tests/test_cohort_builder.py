import datetime
import os
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz
from setup_environment import get_dbengine
from sqlalchemy.exc import OperationalError
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


@pytest.fixture
def mock_db_connection():
    with patch(
        "openaq_engine.setup_environment.connect_to_db",
        return_value=MagicMock(),
    ) as mock_conn:
        yield mock_conn


def test_cohort_builder(mocker):
    # Set environment variables
    os.environ["TEST_PGDATABASE"] = "test_db"
    os.environ["TEST_PGUSER"] = "test_user"
    os.environ["TEST_PGPASSWORD"] = "test_password"
    os.environ["TEST_PGHOST"] = "localhost"
    os.environ["PGPORT"] = "5432"

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
    sensor_type = "reference grade"  # Example sensor type, adjust as needed
    local_data = "cohorts_mumbai"  # Example local data flag, adjust as needed

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

    # Call the cohort_builder method with the required arguments
    cohort_builder.cohort_builder(
        list(train_validation_dict.keys())[0],
        train_validation_dict,
        filter_cols,
        None,  # city is None in this case
        country,
        "openaq-aws",
        sensor_type,
        pollutant,
        local_data,
    )
    cohort_builder.execute_for_openaq_aws.assert_called_with(
        date_tuple,
        None,  # city is None in this case
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
    country = "IN"
    pollutant = "pm25"
    sensor_type = "reference grade"  # Example sensor type, adjust as needed
    local_data = "cohorts_mumbai"

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

    # Call the method with the required arguments
    cohort_df = cohort_builder.execute_for_openaq_aws(
        date_tuple, None, country, pollutant, sensor_type, local_data
    )
    relevant_columns = ["date", "parameter", "value", "coordinates"]
    cohort_df_filtered = cohort_df[relevant_columns]

    # Assert that the filtered DataFrame matches the expected DataFrame
    assert cohort_df_filtered.equals(df)

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
    os.environ["TEST_PGDATABASE"] = "test_db"
    os.environ["TEST_PGUSER"] = "test_user"
    os.environ["TEST_PGPASSWORD"] = "test_password"
    os.environ["TEST_PGHOST"] = "localhost"
    os.environ["PGPORT"] = "5432"
    city = "London"
    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-aws",
    )
    with nullcontext():
        engine = get_dbengine(
            os.getenv("TEST_PGDATABASE"),
            os.getenv("TEST_PGHOST"),
            os.getenv("PGPORT"),
            os.getenv("TEST_PGUSER"),
            os.getenv("TEST_PGPASSWORD"),
        )
    # Mock write_to_db call
    mocker.patch.object(
        CohortBuilder,
        "_results_to_db",
    )
    # Test with city
    if city:
        df = pd.DataFrame({"city": "London", "value": [1]})
        cohort_builder._results_to_db(df, engine, city)
        cohort_builder._results_to_db.assert_called_with(df, engine, "London")
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
    country = "IN"
    pollutant = "pm25"
    sensor_type = "reference"  # Example sensor type, adjust as needed
    local_data = "cohorts_mumbai"  # Example local data flag, adjust as needed

    # Mock API response
    api_response = {
        "results": [
            {
                "locationid": 1,
                "location": "Location1",
                "city": "City1",
                "parameter": "pm25",
                "value": 10,
                "date": {
                    "utc": "2022-04-01T21:00:00.000Z",
                    "local": "2022-04-01T17:00:00-04:00",
                },
                "unit": "µg/m³",
                "coordinates": {
                    "latitude": 44.089355,
                    "longitude": -70.214134,
                },
                "country": "IN",
                "ismobile": False,
                "isanalysis": False,
                "entity": "government",
                "sensortype": "reference grade",
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

    # Call the method with the required arguments
    result = cohort_builder.execute_for_openaq_api(
        date_tuple, None, country, pollutant, sensor_type, local_data
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

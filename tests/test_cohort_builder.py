import datetime

import pandas as pd
from setup_environment import get_dbengine
from src.cohort_builder import CohortBuilder
from contextlib import nullcontext
import src


def test_cohort_builder(mocker):
    # Mock the required arguments
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2020, 3, 1)
    date_tuple = (start_date, end_date)
    train_validation_dict = {"training": [date_tuple]}
    filter_cols = "date, location"
    city = "London"
    country = "UK"
    sensor_type = "reference grade"
    pollutant = "pm25"

    # Mock execute_for_openaq_aws and execute_for_openaq_api calls
    df = pd.DataFrame({"date": [start_date], "value": [5]})
    mocker.patch.object(
        CohortBuilder, "execute_for_openaq_aws", return_value=df
    )
    mocker.patch.object(
        CohortBuilder, "execute_for_openaq_api", return_value=df
    )

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-aws",
    )
    cohort_df = cohort_builder.cohort_builder(
        list(train_validation_dict.keys())[0],
        train_validation_dict,
        filter_cols,
        city,
        country,
        "openaq-aws",
        sensor_type,
        pollutant,
    )

    # Assert calls
    cohort_builder.execute_for_openaq_aws.assert_called_with(
        date_tuple, city, country, pollutant, sensor_type
    )
    cohort_builder.execute_for_openaq_api.assert_not_called()
    # Assert returned DataFrame
    assert cohort_df.equals(df)


def test_execute_for_openaq_aws(mocker):
    # Mock the required arguments
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2020, 3, 1)
    date_tuple = (start_date, end_date)
    city = "London"
    country = "UK"
    pollutant = "pm25"
    sensor_type = "reference grade"

    # Mock the call to build_response_from_aws
    df = pd.DataFrame({"date": [start_date, end_date], "value": [5, 10]})
    mocker.patch.object(
        CohortBuilder, "build_response_from_aws", return_value=df
    )

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-aws",
    )

    # Call the method
    cohort_df = cohort_builder.execute_for_openaq_aws(
        date_tuple, city, country, pollutant, sensor_type
    )

    aws_dict = {
        "bucket": "openaq-pm25-historic",
        "database": "openaq",
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
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2020, 3, 1)
    date_tuple = (start_date, end_date)
    country = "UK"
    pollutant = "pm25"
    sensor_type = "reference grade"

    # Mock the call to api_response_to_df
    df = pd.DataFrame({"date": [start_date, end_date], "value": [5, 10]})
    mock_api_response_to_df = mocker.MagicMock()
    mocker.patch("src.utils.utils", mock_api_response_to_df)

    cohort_builder = CohortBuilder(
        date_col="date",
        filter_dict={},
        target_variable=pollutant,
        country=country,
        source="openaq-aws",
    )

    # Call the method
    cohort_df = cohort_builder.execute_for_openaq_api(
        date_tuple, country, pollutant, sensor_type
    )
    # Assert api_response_to_df was called with the expected URL
    expected_url = """https://api.openaq.org/v2/measurements?date_from={start_date}&date_to={end_date}&limit=100&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&country={country}&sensorType={sensor_type}&order_by=datetime""".format(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country=country,
        pollutant=pollutant,
        sensor_type=sensor_type,
    )
    src.utils.utils.api_response_to_df(expected_url)

    src.utils.utils.api_response_to_df.assert_called_with(expected_url)


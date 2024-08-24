import json
import time
from datetime import datetime
from typing import Any, List

import boto3
import numpy as np
import pandas as pd
import requests
from pydantic.json import pydantic_encoder
from setup_environment import connect_to_db
from sqlalchemy import text


def read_csv(path: str, **kwargs: Any) -> pd.DataFrame:
    """
    Read csv ensuring that nan's are not parsed
    """

    return pd.read_csv(
        path,
        sep=",",
        low_memory=False,
        encoding="utf-8",
        na_filter=False,
        **kwargs,
    )


def write_csv(df: pd.DataFrame, path: str, **kwargs: Any) -> None:
    """
    Write csv to provided path ensuring that the correct encoding and escape
    characters are applied.

    Needed when csv's have text with html tags in it and lists inside cells.
    """
    df.to_csv(
        path,
        index=False,
        na_rep="",
        sep=",",
        lineterminator="\n",
        encoding="utf-8",
        escapechar="\r",
        **kwargs,
    )


def query_results_from_api(params, query):
    url = query
    headers = params
    response = requests.get(url, headers=headers, timeout=None)
    return response


def api_response_to_df(url):
    """
    Fetch data from the provided URL and return a DataFrame and the full response.
    Implements retry with exponential backoff on rate limiting.
    """
    headers = {"accept": "application/json"}
    max_retries = 5
    retry_count = 0
    base_wait_time = 10  # Base wait time in seconds

    while retry_count < max_retries:
        response = query_results_from_api(headers, url)

        if response.status_code == 200:
            return pd.DataFrame(json.loads(response.text)["results"])
        elif response.status_code == 429:
            wait_time = base_wait_time * (2**retry_count)
            print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retry_count += 1
        else:
            print(
                f"API Error: {response.json().get('detail', 'No details provided.')}"
            )
            return pd.DataFrame()
    print("Max retries exceeded. Unable to fetch data.")
    return pd.DataFrame()


def query_results_from_aws(params, query, wait=True):
    session = boto3.Session()

    client = session.client("athena", params["region"])

    response_query_execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={
            "OutputLocation": f"s3://{params['bucket']}/{params['path']}/"
        },
    )
    if not wait:
        return response_query_execution_id["QueryExecutionId"]
    else:
        response_get_query_details = client.get_query_execution(
            QueryExecutionId=response_query_execution_id["QueryExecutionId"]
        )
        status = "RUNNING"
        iterations = 360000  # 30 mins

        while iterations > 0:
            iterations = iterations - 1
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id[
                    "QueryExecutionId"
                ]
            )
            status = response_get_query_details["QueryExecution"]["Status"][
                "State"
            ]

            if (status == "FAILED") or (status == "CANCELLED"):
                failure_reason = response_get_query_details["QueryExecution"][
                    "Status"
                ]["StateChangeReason"]
                print(failure_reason)
                return False, False

            elif status == "SUCCEEDED":
                # Function to get output results
                response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id[
                        "QueryExecutionId"
                    ]
                )
                print("response_query_result", response_query_result)
                return response_query_result

        else:
            time.sleep(0.001)

        return False


def get_s3_file_path_list(resource, bucket, folder):
    csv_filetype = ".csv"
    my_bucket = resource.Bucket(bucket)
    csv_list = []
    for object_summary in my_bucket.objects.filter(Prefix=f"{folder}"):
        if object_summary.key.endswith(csv_filetype):
            csv_list.append(object_summary.key)

    return csv_list


def write_dataclass(dclass: object, path: str) -> None:
    """
    Write a dataclass to the provided path as a json

    """
    with open(path, "w+") as f:
        f.write(
            json.dumps(
                dclass, indent=4, ensure_ascii=True, default=pydantic_encoder
            )
        )


def get_categorical_feature_indices(df: pd.DataFrame) -> List[int]:
    return list(np.where(df.dtypes == "category")[0])


def json_provider(file_path, cmd_name):
    with open(file_path) as config_data:
        return json.load(config_data)


def parametrized(dec):
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)

        return repl

    return layer


def get_data(query):
    """
    Pulls data from the db based on the query
    Input
    -----
    query: str
       SQL query from the database
    Output
    ------
    data: DataFrame
       Dump of Query into a DataFrame
    """

    if isinstance(query, str):
        query = text(
            query
        )  # Only wrap in text if it's a string, not already a TextClause

    with connect_to_db() as conn:
        df = pd.read_sql_query(query, conn)
    return df


def extract_utc_date(date_dict):
    """
    Extracts the UTC date from the date dictionary and converts it to a datetime.date object.

    Parameters:
    date_dict (dict): The dictionary containing date information with 'utc' and 'local' keys.

    Returns:
    datetime.date: The date part of the 'utc' datetime.
    """
    # If the input is a string, parse it as JSON
    if isinstance(date_dict, str):
        date_dict = json.loads(date_dict)

    utc_datetime_str = date_dict["utc"]
    utc_datetime = datetime.fromisoformat(
        utc_datetime_str.replace("Z", "+00:00")
    )
    return utc_datetime.date()


def write_to_db(
    df,
    engine,
    table_name,
    schema_name,
    table_behaviour,
    index=False,
    **kwargs,
):
    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists=table_behaviour,
        index=index,
        **kwargs,
    )


def ee_array_to_df(arr, list_of_bands):
    """Transforms client-side ee.Image.getRegion array to pandas.DataFrame."""
    df = pd.DataFrame(arr[1:], columns=arr[0])

    # Remove rows without data inside.
    df = df[["longitude", "latitude", "time", *list_of_bands]].dropna()

    # Convert the data to numeric values.
    for band in list_of_bands:
        df[band] = pd.to_numeric(df[band], errors="coerce")

    # Convert the time field into a datetime.
    df["datetime"] = pd.to_datetime(df["time"], unit="ms")

    # Keep the columns of interest.
    df = df[["longitude", "latitude", "time", "datetime", *list_of_bands]]

    return df


def load_data_for_single_tv_set(cohort_table, tv_set):
    # SQL query for X_train and Y_train filtered by tv_set
    train_query = f"""
    SELECT
        EXTRACT(EPOCH FROM "datetime_hour") AS "timestamp_as_float",
        "y",
        "x",
        "Optical_Depth_047",
        "Optical_Depth_047_time_diff",
        "SR_B4",
        "SR_B4_time_diff",
        "SR_B3",
        "SR_B2",
        "avg_rad",
        "avg_rad_time_diff",
        "temperature_2m_above_ground",
        "temperature_2m_above_ground_time_diff",
        "relative_humidity_2m_above_ground",
        "precipitable_water_entire_atmosphere",
        "u_component_of_wind_10m_above_ground",
        "v_component_of_wind_10m_above_ground",
        "value"
    FROM
        "{cohort_table}_training"
    WHERE
        {tv_set} = ANY("tv_set"::int[]);
    """

    # SQL query for X_valid and Y_valid filtered by tv_set
    valid_query = f"""
    SELECT
        EXTRACT(EPOCH FROM "datetime_hour") AS "timestamp_as_float",
        "y",
        "x",
        "Optical_Depth_047",
        "Optical_Depth_047_time_diff",
        "SR_B4",
        "SR_B4_time_diff",
        "SR_B3",
        "SR_B2",
        "avg_rad",
        "avg_rad_time_diff",
        "temperature_2m_above_ground",
        "temperature_2m_above_ground_time_diff",
        "relative_humidity_2m_above_ground",
        "precipitable_water_entire_atmosphere",
        "u_component_of_wind_10m_above_ground",
        "v_component_of_wind_10m_above_ground",
        "value"
    FROM
        "{cohort_table}_validation"
    WHERE
        {tv_set} = ANY("tv_set"::int[]);
    """

    # Retrieve data from the database
    train_df = get_data(train_query)
    valid_df = get_data(valid_query)

    # Separate features (X) and labels (Y)
    X_train = train_df.drop(columns=["value"])
    Y_train = train_df["value"]

    X_valid = valid_df.drop(columns=["value"])
    Y_valid = valid_df["value"]

    return X_train, Y_train, X_valid, Y_valid

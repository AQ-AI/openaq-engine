import json
import time
from typing import Any, List

import boto3
import numpy as np
import pandas as pd
import requests
from pydantic.json import pydantic_encoder
from setup_environment import connect_to_db


def read_csv(path: str, **kwargs: Any) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame ensuring that NaNs are not parsed.

    Parameters
    ----------
    path : str
        Path to the CSV file.
    **kwargs : Any
        Additional arguments to pass to `pd.read_csv`.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the data from the CSV file.
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
    Write a DataFrame to a CSV file with specified encoding and escape characters.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write to CSV.
    path : str
        Path to the output CSV file.
    **kwargs : Any
        Additional arguments to pass to `pd.DataFrame.to_csv`.

    Returns
    -------
    None
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


def query_results_from_api(headers: dict, url: str) -> requests.Response:
    """
    Query an API and return the response.

    Parameters
    ----------
    headers : dict
        Headers to include in the API request.
    url : str
        URL to query.

    Returns
    -------
    requests.Response
        Response object from the API.
    """
    response = requests.get(url, headers=headers)
    return response


def api_response_to_df(url: str) -> pd.DataFrame:
    """
    Query an API and convert the response to a DataFrame.

    Parameters
    ----------
    url : str
        URL to query.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the API response data.
    """
    headers = {"accept": "application/json"}
    response = query_results_from_api(headers, url)
    try:
        return pd.DataFrame(response.json()["results"])
    except KeyError:
        pass


def query_results_from_aws(params: dict, query: str, wait: bool = True) -> Any:
    """
    Query AWS Athena and return the results.

    Parameters
    ----------
    params : dict
        Dictionary containing AWS parameters.
    query : str
        SQL query to execute on AWS Athena.
    wait : bool, optional
        Whether to wait for the query to complete, by default True.

    Returns
    -------
    Any
        Query execution ID or query results, depending on the wait parameter.
    """
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
        status = "RUNNING"
        iterations = 360000  # 30 mins

        while iterations > 0:
            iterations -= 1
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id[
                    "QueryExecutionId"
                ]
            )
            status = response_get_query_details["QueryExecution"]["Status"][
                "State"
            ]

            if status in ["FAILED", "CANCELLED"]:
                failure_reason = response_get_query_details["QueryExecution"][
                    "Status"
                ]["StateChangeReason"]
                print(failure_reason)
                return False, False

            elif status == "SUCCEEDED":
                response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id[
                        "QueryExecutionId"
                    ]
                )
                return response_query_result

            time.sleep(0.001)

        return False


def get_s3_file_path_list(
    resource: Any, bucket: str, folder: str
) -> List[str]:
    """
    Get a list of file paths from an S3 bucket.

    Parameters
    ----------
    resource : Any
        Boto3 resource object.
    bucket : str
        Name of the S3 bucket.
    folder : str
        Folder path within the S3 bucket.

    Returns
    -------
    List[str]
        List of file paths in the specified S3 bucket folder.
    """
    csv_filetype = ".csv"
    my_bucket = resource.Bucket(bucket)
    csv_list = []
    for object_summary in my_bucket.objects.filter(Prefix=f"{folder}"):
        if object_summary.key.endswith(csv_filetype):
            csv_list.append(object_summary.key)

    return csv_list


def write_dataclass(dclass: object, path: str) -> None:
    """
    Write a dataclass object to a JSON file.

    Parameters
    ----------
    dclass : object
        Dataclass object to write.
    path : str
        Path to the output JSON file.

    Returns
    -------
    None
    """
    with open(path, "w+") as f:
        f.write(
            json.dumps(
                dclass, indent=4, ensure_ascii=True, default=pydantic_encoder
            )
        )


def get_categorical_feature_indices(df: pd.DataFrame) -> List[int]:
    """
    Get the indices of categorical features in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to analyze.

    Returns
    -------
    List[int]
        List of indices of categorical features.
    """
    return list(np.where(df.dtypes == "category")[0])


def json_provider(file_path: str, cmd_name: str) -> dict:
    """
    Load a JSON file.

    Parameters
    ----------
    file_path : str
        Path to the JSON file.
    cmd_name : str
        Command name for logging purposes (optional).

    Returns
    -------
    dict
        Dictionary containing the JSON data.
    """
    with open(file_path) as config_data:
        return json.load(config_data)


def parametrized(dec):
    """
    Decorator for parameterizing functions.

    Parameters
    ----------
    dec : callable
        Decorator function.

    Returns
    -------
    callable
        Decorated function.
    """

    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)

        return repl

    return layer


def get_data(query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the results as a DataFrame.

    Parameters
    ----------
    query : str
        SQL query to execute.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the query results.
    """
    with connect_to_db() as conn:
        df = pd.read_sql_query(query, conn)
    return df


def write_to_db(
    df: pd.DataFrame,
    engine: Any,
    table_name: str,
    schema_name: str,
    table_behaviour: str,
    index: bool = False,
    **kwargs: Any,
) -> None:
    """
    Write a DataFrame to a database table.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write.
    engine : Any
        SQLAlchemy engine object.
    table_name : str
        Name of the table.
    schema_name : str
        Schema name.
    table_behaviour : str
        'replace', 'append', or 'fail'.
    index : bool, optional
        Whether to write row indices, by default False.
    **kwargs : Any
        Additional arguments to pass to `pd.to_sql`.

    Returns
    -------
    None
    """
    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists=table_behaviour,
        index=index,
        **kwargs,
    )


def ee_array_to_df(arr: List[Any], list_of_bands: List[str]) -> pd.DataFrame:
    """
    Transform an Earth Engine array to a DataFrame.

    Parameters
    ----------
    arr : List[Any]
        Array from Earth Engine.
    list_of_bands : List[str]
        List of bands to include.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the transformed data.
    """
    df = pd.DataFrame(arr)

    # Rearrange the header.
    headers = df.iloc(0).tolist()  # Ensure headers are in list format
    df = pd.DataFrame(df.values[1:], columns=headers)

    # Remove rows without data inside.
    df = df[["longitude", "latitude", "time", *list_of_bands]].dropna()

    # Convert the data to numeric values.
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    for band in list_of_bands:
        df[band] = pd.to_numeric(df[band], errors="coerce")

    # Convert the time field into a datetime.
    df["datetime"] = pd.to_datetime(df["time"], unit="ms")

    # Keep the columns of interest.
    df = df[["longitude", "latitude", "time", "datetime", *list_of_bands]]

    return df.reset_index(drop=True)

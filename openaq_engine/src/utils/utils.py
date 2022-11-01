import time
import os
import json
from typing import Any, List, Union
import numpy as np
import pandas as pd
from pydantic.json import pydantic_encoder
import boto3
from datetime import timedelta

import pandas as pd
from setup_environment import connect_to_db


def date_range(start, end):
    delta = end - start  # as timedelta
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days


def read_csv(path: str, **kwargs: Any) -> pd.DataFrame:
    """
    Read csv ensuring that nan's are not parsed
    """

    return pd.read_csv(
        path, sep=",", low_memory=False, encoding="utf-8", na_filter=False, **kwargs
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
        line_terminator="\n",
        encoding="utf-8",
        escapechar="\r",
        **kwargs,
    )


def query_results(params, query, wait=True):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    session = boto3.Session()

    client = session.client("athena", params["region"])

    response_query_execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={
            "OutputLocation": "s3://" + params["bucket"] + "/" + params["path"] + "/"
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
                QueryExecutionId=response_query_execution_id["QueryExecutionId"]
            )
            status = response_get_query_details["QueryExecution"]["Status"]["State"]

            if (status == "FAILED") or (status == "CANCELLED"):
                failure_reason = response_get_query_details["QueryExecution"]["Status"][
                    "StateChangeReason"
                ]
                print(failure_reason)
                return False, False

            elif status == "SUCCEEDED":
                location = response_get_query_details["QueryExecution"][
                    "ResultConfiguration"
                ]["OutputLocation"]

                ## Function to get output results
                response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id["QueryExecutionId"]
                )
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
            json.dumps(dclass, indent=4, ensure_ascii=True, default=pydantic_encoder)
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

    with connect_to_db() as conn:
        df = pd.read_sql_query(query, conn)
    return df


def write_to_db(
    df, engine, table_name, schema_name, table_behaviour, index=False, **kwargs
):

    with engine.connect() as conn:
        with conn.begin():
            df.pg_copy_to(
                name=table_name,
                schema=schema_name,
                con=conn,
                if_exists=table_behaviour,
                index=index,
                **kwargs,
            )


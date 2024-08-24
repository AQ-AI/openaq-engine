#!/usr/bin/env python
"""
Setup Environment

Tools for connecting to the database.
"""

import os
from contextlib import contextmanager

import pandas as pd
from sqlalchemy.engine import create_engine


def get_athena_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to AWS Athena.

    Returns
    -------
    engine : SQLAlchemy Engine
        A SQLAlchemy engine connected to AWS Athena.
    """
    conn_str = (
        "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}"
        "@athena.{region_name}.amazonaws.com:443/"
        "{schema_name}?s3_staging_dir={s3_staging_dir}"
    )

    engine = create_engine(
        conn_str.format(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1",
            schema_name="default",
            s3_staging_dir="s3://openaq-pm25-historic/pm25-month/cohorts/",
        )
    )
    return engine


def get_dbengine(
    database=None, host=None, port=None, user=None, password=None
):
    database = database or os.getenv("TEST_PGDATABASE")
    user = user or os.getenv("TEST_PGUSER")
    password = password or os.getenv("TEST_PGPASSWORD")
    host = host or os.getenv("TEST_PGHOST")
    port = port or os.getenv("TEST_PGPORT")

    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine


@contextmanager
def connect_to_db():
    # Check if the USE_TEST_DB environment variable is set to "true"
    use_test_db = os.getenv("USE_TEST_DB", "false").lower() == "true"

    if use_test_db:
        database = os.getenv("TEST_PGDATABASE")
        user = os.getenv("TEST_PGUSER")
        password = os.getenv("TEST_PGPASSWORD")
        host = os.getenv("TEST_PGHOST")
        port = os.getenv("TEST_PGPORT")
    else:
        database = os.getenv("PGDATABASE")
        user = os.getenv("PGUSER")
        password = os.getenv("PGPASSWORD")
        host = os.getenv("PGHOST")
        port = os.getenv("PGPORT")

    engine = get_dbengine(database, host, port, user, password)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()


def run_query(query):
    """
    Executes a SQL query on the database and returns the result as a pandas DataFrame.

    Parameters
    ----------
    query : str
        The SQL query to execute.

    Returns
    -------
    data : pandas DataFrame
        A DataFrame containing the results of the query.
    """
    with connect_to_db() as conn:
        data = pd.read_sql(query, conn)
    return data


def test_database_connect():
    """
    Tests the database connection by running a simple query.

    Raises
    ------
    AssertionError
        If the query returns fewer than 1 row.
    """
    with connect_to_db() as conn:
        query = "SELECT * FROM raw.codes LIMIT 10"
        data = pd.read_sql_query(query, conn)
        assert len(data) > 1

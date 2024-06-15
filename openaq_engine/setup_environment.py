#!/usr/bin/env python
"""
Setup Enviroment

Tools for connecting to the
database.

"""

import os
from contextlib import contextmanager

import pandas as pd
from sqlalchemy.engine import create_engine


def get_athena_engine():
    """
    Returns a sql engine

    Output
    ------
    engine: SQLalchemy engine
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
    port = port or os.getenv("PGPORT")

    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine


@contextmanager
def connect_to_db(use_test_db=False):
    if use_test_db:
        database = os.getenv("TEST_PGDATABASE")
        user = os.getenv("TEST_PGUSER")
        password = os.getenv("TEST_PGPASSWORD")
        host = os.getenv("TEST_PGHOST")
        port = os.getenv("PGPORT")
    else:
        database = os.getenv("PGDATABASE")
        user = os.getenv("PGUSER")
        password = os.getenv("PGPASSWORD")
        host = os.getenv("PGHOST")
        port = os.getenv("PGPORT")

    engine = get_dbengine(database, user, password, host, port)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()


def run_query(query):
    """
    Runs a query on the database and returns
    the result in a dataframe.
    """
    with connect_to_db() as conn:
        data = pd.read_sql(query, conn)
    return data


def test_database_connect():
    """
    test database connection
    """
    with connect_to_db() as conn:
        query = "select * from raw.codes limit 10"
        data = pd.read_sql_query(query, conn)
        assert len(data) > 1

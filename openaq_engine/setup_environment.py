#!/usr/bin/env python
"""
Setup Enviroment

Tools for connecting to the
database.

"""

import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
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


def get_dbengine(PGDATABASE, PGHOST, PGPORT, PGUSER, PGPASSWORD):
    url = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    engine = create_engine(url)
    return engine


@contextmanager
def connect_to_db(PGDATABASE=None, PGPORT=5432, use_test_db=False):
    """
    Connects to database
    Output
    ------
    conn: object
       Database connection.
    """
    if use_test_db:
        PGDATABASE = os.getenv("TEST_PGDATABASE")
        PGUSER = os.getenv("TEST_PGUSER")
        PGPASSWORD = os.getenv("TEST_PGPASSWORD")
        PGHOST = os.getenv("TEST_PGHOST")
    else:
        PGDATABASE = PGDATABASE or os.getenv("PGDATABASE")
        PGUSER = os.getenv("PGUSER")
        PGPASSWORD = os.getenv("PGPASSWORD")
        PGHOST = os.getenv("PGHOST")

    try:
        engine = get_dbengine(
            PGDATABASE=PGDATABASE,
            PGHOST=PGHOST,
            PGPORT=PGPORT,
            PGUSER=PGUSER,
            PGPASSWORD=PGPASSWORD,
        )
        conn = engine.connect()
        yield conn
    except psycopg2.Error:
        raise SystemExit("Cannot Connect to DB")
    else:
        conn.close()


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

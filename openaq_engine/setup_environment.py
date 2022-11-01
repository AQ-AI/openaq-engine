#!/usr/bin/env python
"""
Setup Enviroment

Tools for connecting to the
database.

"""

from contextlib import contextmanager
import os
import pandas as pd
import psycopg2
import yaml
from pkg_resources import resource_filename
from sqlalchemy.engine import create_engine


def get_dbengine(PGPORT=5432, DBTYPE="postgresql"):
    """
    Returns a sql engine

    Input
    -----
    PGDATABASE: str
            DB Name
    PGHOST: str
            hostname
    PGPASSWORD: str
            DB password
    DBTYPE: str
            type of database, default is posgresql

    Output
    ------
    engine: SQLalchemy engine
    """
    conn_str = 'awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/'\
            '{schema_name}?s3_staging_dir={s3_staging_dir}'

    engine = create_engine(conn_str.format(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name='us-east-1',
        schema_name="default",
        s3_staging_dir="s3://s3-results-bucket/output/",
    ))
    return engine


@contextmanager
def connect_to_db(PGPORT=5432):
    """
    Connects to database
    Output
    ------
    conn: object
       Database connection.
    """
    try:
        engine = get_dbengine(
            PGDATABASE=os.getenv("PGDATABASE"),
            PGHOST=os.getenv("PGHOST"),
            PGPORT=PGPORT,
            PGUSER=os.getenv("PGUSER"),
            PGPASSWORD=os.getenv("PGPASSWORD"),
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
    with connect_to_db(**db_dict) as conn:
        data = pd.read_sql(query, conn)
    return data


def test_database_connect():
    """
    test database connection
    """
    with connect_to_db(**db_dict) as conn:
        query = "select * from raw.codes limit 10"
        data = pd.read_sql_query(query, conn)
        assert len(data) > 1

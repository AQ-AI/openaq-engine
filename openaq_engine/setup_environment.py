#!/usr/bin/env python
"""
Setup Environment

Tools for connecting to the database.
"""

import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
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
    PGDATABASE="",
    PGHOST="",
    PGPORT=5432,
    PGPASSWORD="",
    PGUSER="",
    DBTYPE="postgresql",
):
    """
    Creates and returns a SQLAlchemy engine for connecting to a PostgreSQL database.

    Parameters
    ----------
    PGDATABASE : str
        The name of the database to connect to.
    PGHOST : str
        The hostname of the database server.
    PGPORT : int, optional
        The port number to connect to (default is 5432).
    PGPASSWORD : str
        The password for the database user.
    PGUSER : str
        The username for the database.
    DBTYPE : str, optional
        The type of database, default is "postgresql".

    Returns
    -------
    engine : SQLAlchemy Engine
        A SQLAlchemy engine connected to the specified database.
    """
    str_conn = "{dbtype}://{username}@{host}:{port}/{db}".format(
        dbtype=DBTYPE,
        username=os.getenv("PGUSER"),
        db=os.getenv("PGDATABASE"),
        host=os.getenv("PGHOST"),
        port=PGPORT,
    )

    return create_engine(str_conn)


@contextmanager
def connect_to_db(PGPORT=5432):
    """
    Context manager for connecting to a PostgreSQL database.

    Parameters
    ----------
    PGPORT : int, optional
        The port number to connect to (default is 5432).

    Yields
    ------
    conn : SQLAlchemy Connection
        A connection to the PostgreSQL database.
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

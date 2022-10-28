import os
from dataclasses import field
from typing import Any, Dict, List, Sequence

from pydantic.dataclasses import dataclass
from src.utils.utils import date_range

import boto3
import pandas as pd


@dataclass
class HistoricOpenAQConfig:
    # athena constant
    DATABASE = os.getenv("DB_NAME_OPENAQ")
    TABLE = "openaq"
    REGION = "us-east-1"
    # S3 constant
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")

    # number of retries

    # query constant
    PARAMETER = "pm25"

    DATES = pd.date_range("2020-01-01", "2022-01-01", freq="M")
    TIME_AGGREGATION = 4


class RealtimeOpenAQConfig:
    DATABASE = os.getenv("DB_NAME_OPENAQ")
    TABLE = "openaq"
    REGION = "us-east-1"
    # S3 constant
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")


@dataclass
class CohortBuilderConfig:
    ENTITY_ID_COLS: Sequence[str] = field(default_factory=lambda: ["unique_id"])
    DATE_COL: str = "date.utc"
    DATABASE = "openaq_db"
    REGION = "us-east-1"
    TABLE_NAME: str = "openaq"
    SCHEMA_NAME: str = "model_output"
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_null_pollution_values=["parameter"],
            # filter_non_standard_codes=["category"],
        ),
    )
    PRIORITY_SCHEMA_NAME = "raw"
    PRIORITY_TABLE_NAME = "priority_codes"
    NO_OF_OCCURENCES = 500
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")


@dataclass
class TimeSplitterConfig:
    DATE_COL: str = "triage_datetime"
    TIME_WINDOW_LENGTH: int = 12
    WITHIN_WINDOW_SAMPLER: int = 3
    WINDOW_COUNT: int = 3  # this will increase for more than one split
    TABLE_NAME: str = "openaq"
    REGION = "us-east-1"
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    RESOURCE = boto3.resource("s3")


@dataclass
class CohortBuilderConfig:
    ENTITY_ID_COLS: Sequence[str] = field(default_factory=lambda: ["unique_id"])
    DATE_COL: str = "triage_datetime"
    TABLE_NAME: str = "train"
    SCHEMA_NAME: str = "model_prep"

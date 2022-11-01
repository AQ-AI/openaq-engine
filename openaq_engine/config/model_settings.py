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
    TABLE_NAME = "openaq"
    SCHEMA_NAME: str = "model_output"
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_non_null_pm25_values=["parameter"],
            filter_extreme=["parameter"],
        ),
    )
    PRIORITY_SCHEMA_NAME = "raw"
    PRIORITY_TABLE_NAME = "priority_codes"
    NO_OF_OCCURENCES = 500
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")


@dataclass
class TimeSplitterConfig:
    DATE_COL: str = "date.utc"
    TARGET_VARIABLE = "pm25"
    TIME_WINDOW_LENGTH: int = 12
    WITHIN_WINDOW_SAMPLER: int = 3
    WINDOW_COUNT: int = 3  # this will increase for more than one split
    TABLE_NAME: str = "openaq"
    REGION = "us-east-1"
    DATABASE = os.getenv("DB_NAME_OPENAQ")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    RESOURCE = boto3.resource("s3")
    TRAIN_VALIDATION_DICT: Dict[str, List[Any]] = field(
        default_factory=lambda: dict(
            validation=[],
            training=[],
        )
    )

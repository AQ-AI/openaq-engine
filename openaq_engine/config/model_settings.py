import os
from dataclasses import field
from typing import Sequence

from pydantic.dataclasses import dataclass
from typing import Any, Dict, List

import boto3
import pandas as pd


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

@dataclass
class CohortBuilderConfig:
    ENTITY_ID_COLS: Sequence[str] = field(
        default_factory=lambda: ["unique_id"]
    )
    DATE_COL: str = "triage_datetime"
    TABLE_NAME: str = "train"
    SCHEMA_NAME: str = "model_prep"

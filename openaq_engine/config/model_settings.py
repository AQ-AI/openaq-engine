import os
from dataclasses import field
from typing import Any, Dict, List, Sequence

import boto3
from pydantic import StrictStr
from pydantic.dataclasses import dataclass


@dataclass
class FeatureConfig:
    CATEGORICAL_FEATURES: List[StrictStr] = field(
        default_factory=lambda: [
            "city",
            "country",
            "sourcetype",
        ]
    )
    CORE_FEATURES: List[StrictStr] = field(
        default_factory=lambda: [
            "city",
            "country",
            "pca_lat",
            "pca_lng",
            "sourcetype",
            "mobile",
        ]
    )
    SATELLITE_FEATURES = []

    @property
    def ALL_MODEL_FEATURES(self) -> List[str]:
        """Return all features to be fed into the model"""
        return list(
            set((self.CORE_FEATURES + self.CATEGORICAL_FEATURES))
            - set(self.EXCLUDE_FEATURES)
        )


@dataclass
class CohortBuilderConfig:
    ENTITY_ID_COLS: Sequence[str] = field(
        default_factory=lambda: ["unique_id"]
    )
    DATE_COL: str = "date.utc"
    REGION = "us-east-1"
    TABLE_NAME = "openaq"
    SCHEMA_NAME: str = "model_output"
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_pollutant=["parameter"],
            filter_non_null_values=["value"],
            filter_extreme_values=["value"],
            # filter_countries=["country"],
            # filter_cities=["city"],
        ),
    )
    POLLUTANT_TO_PREDICT = "pm25"
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

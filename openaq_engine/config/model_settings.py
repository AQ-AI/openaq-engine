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
class EEConfig:
    DATE_COL: str = "date"
    TABLE_NAME = "cohorts"
    LANDSAT_IMAGE_COLLECTION: str = "LANDSAT/LC08/C01/T1"
    LANDSAT_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["B4", "B3", "B2"]
    )

    NIGHTTIME_LIGHT_IMAGE_COLLECTION: str = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG"
    NIGHTTIME_LIGHT_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["avg_rad"]
    )

    METEROLOGICAL_IMAGE_COLLECTION: str = "NOAA/GFS0P25"
    METEROLOGICAL_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: [
            "temperature_2m_above_ground",
            "relative_humidity_2m_above_ground",
            "total_precipitation_surface",
            "total_cloud_cover_entire_atmosphere",
            "u_component_of_wind_10m_above_ground",
            "v_component_of_wind_10m_above_ground",
        ]
    )

    POPULATION_IMAGE_COLLECTION: str = (
        "CIESIN/GPWv411/GPW_Basic_Demographic_Characteristics"
    )
    POPULATION_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["basic_demographic_characteristics"]
    )

    LAND_COVER_IMAGE_COLLECTION: str = "COPERNICUS/Landcover/100m/Proba-V-C3/Global"
    LAND_COVER_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["discrete_classification"]
    )
    BUCKET_NAME = "earthengine-bucket"

    @property
    def ALL_SATELITTES(self) -> zip(List[str], List[str]):
        """Return all features to be fed into the model"""
        return zip(
            [
                self.LANDSAT_IMAGE_COLLECTION,
                self.NIGHTTIME_LIGHT_IMAGE_COLLECTION,
                self.METEROLOGICAL_IMAGE_COLLECTION,
                self.POPULATION_IMAGE_COLLECTION,
                self.LAND_COVER_IMAGE_COLLECTION,
            ],
            [
                self.LANDSAT_IMAGE_BAND,
                self.NIGHTTIME_LIGHT_IMAGE_BAND,
                self.METEROLOGICAL_IMAGE_BAND,
                self.POPULATION_IMAGE_BAND,
                self.LAND_COVER_IMAGE_BAND,
            ],
        )


@dataclass
class CohortBuilderConfig:
    ENTITY_ID_COLS: Sequence[str] = field(default_factory=lambda: ["unique_id"])
    DATE_COL: str = "date.utc"
    REGION = "us-east-1"
    TABLE_NAME = "openaq"
    SCHEMA_NAME: str = "model_output"
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_pollutant=["parameter"],
            filter_non_null_values=["value"],
            filter_extreme_values=["value"],
            filter_no_coordinates=["coordinates"],
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

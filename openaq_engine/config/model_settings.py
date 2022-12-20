import os
from dataclasses import field
from typing import Any, Dict, List, Sequence

import boto3
from pydantic import StrictStr
from pydantic.dataclasses import dataclass


@dataclass
class BuildFeaturesConfig:
    TARGET_COL: str = "value"
    CATEGORICAL_FEATURES: List[StrictStr] = field(default_factory=lambda: [])
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
        return list(set(self.CORE_FEATURES + self.CATEGORICAL_FEATURES))


@dataclass
class EEConfig:
    DATE_COL: str = "timestamp_utc"
    TABLE_NAME = "cohorts"

    AOD_IMAGE_COLLECTION: str = "MODIS/006/MCD19A2_GRANULES"
    AOD_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["Optical_Depth_047"]
    )
    AOD_IMAGE_PERIOD = 2
    AOD_IMAGE_RES = 1000
    LANDSAT_IMAGE_COLLECTION: str = "LANDSAT/LC08/C01/T1"
    LANDSAT_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["B4", "B3", "B2"]
    )
    LANDSAT_PERIOD = 8
    LANDSAT_RES = 30
    NIGHTTIME_LIGHT_IMAGE_COLLECTION: str = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG"
    NIGHTTIME_LIGHT_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["avg_rad"]
    )
    NIGHTTIME_LIGHT_PERIOD = 30
    NIGHTTIME_LIGHT_RES = 463.83
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
    METEROLOGICAL_IMAGE_PERIOD = 1
    METEROLOGICAL_IMAGE_RES = 27830
    POPULATION_IMAGE_COLLECTION: str = (
        "CIESIN/GPWv411/GPW_Basic_Demographic_Characteristics"
    )
    POPULATION_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["basic_demographic_characteristics"]
    )
    POPULATION_IMAGE_RES = 1000
    LAND_COVER_IMAGE_COLLECTION: str = (
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global"
    )
    LAND_COVER_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["discrete_classification"]
    )
    LAND_COVER_IMAGE_RES = 100
    BUCKET_NAME = "earthengine-bucket"
    PATH_TO_PRIVATE_KEY = "private_keys/unicef-367711-29676476912d.json"
    SERVICE_ACCOUNT = "earth-engine@unicef-367711.iam.gserviceaccount.com"

    @property
    def VARIABLE_SATELLITES(self) -> zip(List[str], List[str]):
        """Return varying satellites to be fed into the model"""
        return zip(
            [
                self.AOD_IMAGE_COLLECTION,
                self.LANDSAT_IMAGE_COLLECTION,
                self.NIGHTTIME_LIGHT_IMAGE_COLLECTION,
                self.METEROLOGICAL_IMAGE_COLLECTION,
            ],
            [
                self.AOD_IMAGE_BAND,
                self.LANDSAT_IMAGE_BAND,
                self.NIGHTTIME_LIGHT_IMAGE_BAND,
                self.METEROLOGICAL_IMAGE_BAND,
            ],
            [
                self.AOD_IMAGE_PERIOD,
                self.LANDSAT_PERIOD,
                self.NIGHTTIME_LIGHT_PERIOD,
                self.METEROLOGICAL_IMAGE_PERIOD,
            ],
            [
                self.AOD_IMAGE_RES,
                self.LANDSAT_RES,
                self.NIGHTTIME_LIGHT_RES,
                self.METEROLOGICAL_IMAGE_RES,
            ],
        )

    @property
    def STATIC_SATELLITES(self) -> zip(List[str], List[str]):
        return zip(
            [
                self.POPULATION_IMAGE_COLLECTION,
                self.LAND_COVER_IMAGE_COLLECTION,
            ],
            [
                self.POPULATION_IMAGE_BAND,
                self.LAND_COVER_IMAGE_BAND,
            ],
            [
                self.POPULATION_IMAGE_RES,
                self.LAND_COVER_IMAGE_RES,
            ],
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

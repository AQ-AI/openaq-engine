import os
from dataclasses import field
from typing import Any, Dict, List, Sequence

import boto3
from pydantic import StrictStr
from pydantic.dataclasses import dataclass


@dataclass
class ModelVisualizerConfig:
    PLOT: bool = True
    PLOT_METRICS: Sequence[str] = field(default_factory=lambda: ["mean"])
    PLOTS_TABLE_NAME: str = ""
    PLOTS_SCHEMA_NAME: str = ""
    RESULTS_TABLE_NAME: str = "results"


@dataclass
class MatrixGeneratorConfig:
    ALGORITHM = "RFR"
    ID_COLUMN_LIST: Sequence[str] = field(
        default_factory=lambda: ["locationId", "cohort", "cohort_type"]
    )


@dataclass
class ModelTrainerConfig:
    MODEL_NAMES_LIST = ["RFR"]  # "DTC", "MNB", "RFC", "MLR"
    ID_COLS_TO_REMOVE = [
        "location_id",
        "cohort",
        "cohort_type",
    ]
    RANDOM_STATE = 99


@dataclass
class ModelEvaluatorConfig:
    METRICS: Sequence[str] = field(default_factory=lambda: ["mse", "mape"])

    SUMMARY_METHOD = "summary"
    VALID_MODELS: Sequence[str] = field(
        default_factory=lambda: ["DTC", "RFR", "XGB", "MNB", "MLR"]
    )


@dataclass
class HyperparamConfig:
    MODEL_TYPES = ["DTC", "MNB", "RFR", "XGB"]
    MODEL_HYPERPARAMS = {
        "DTC": {
            "max_depth": [5, 10, 20, 30, 40]
        },  # 5, 50, 500, 10000 50, 100, 200, 300
        "RFR": {
            "n_estimators": [500, 800],  # 100, 500, 800, 1000
            "max_depth": [10, 50, 70],  # 5, 50, 80, 500, 10000  100, 200, 300
        },
        "XGB": {
            "max_depth": [5, 150, 200, 250, 300],
            "learning_rate": [0.1, 0.5, 1],
        },
        "MNB": {"alpha": [0, 0.05]},  # 0.1, 0.5, 0.8, 1
        "MLR": {
            "penalty": ["l2"],
            "C": [1, 0.1, 0.01],
            "solver": ["saga"],
            "max_iter": [2000],
        },
    }


@dataclass
class BuildFeaturesConfig:
    TARGET_COL: str = "value"
    TARGET_VARIABLE = "pm25"
    COUNTRY = ""
    CITY = ""
    CATEGORICAL_FEATURES: List[StrictStr] = field(default_factory=lambda: [])
    CORE_FEATURES: List[StrictStr] = field(default_factory=lambda: [])
    SATELLITE_FEATURES: List[StrictStr] = field(
        default_factory=lambda: [
            "Optical_Depth_047",
            "B4",
            "B3",
            "B2",
            "avg_rad",
            "temperature_2m_above_ground",
            "relative_humidity_2m_above_ground",
            "total_precipitation_surface",
            "total_cloud_cover_entire_atmosphere",
            "u_component_of_wind_10m_above_ground",
            "v_component_of_wind_10m_above_ground",
            "discrete_classification",
        ]
    )

    @property
    def ALL_MODEL_FEATURES(self) -> List[str]:
        """Return all features to be fed into the model"""
        return list(
            set(
                self.CORE_FEATURES
                + self.CATEGORICAL_FEATURES
                + self.SATELLITE_FEATURES
            )
        )


@dataclass
class EEConfig:
    LOOKBACK_N = 1
    DATE_COL: str = "timestamp_utc"
    TABLE_NAME = "cohorts"
    # Satellite configurations
    AOD_IMAGE_COLLECTION: str = "MODIS/061/MCD19A2_GRANULES"
    AOD_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["Optical_Depth_047"]
    )
    AOD_IMAGE_PERIOD = 2
    AOD_IMAGE_RES = 1000
    LANDSAT_IMAGE_COLLECTION: str = "LANDSAT/LC08/C02/T1_L2"
    LANDSAT_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["SR_B4", "SR_B3", "SR_B2"]
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
    POPULATION_PERIOD = 1100
    POPULATION_IMAGE_RES = 1000
    LAND_COVER_IMAGE_COLLECTION: str = (
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global"
    )
    LAND_COVER_IMAGE_BAND: Sequence[str] = field(
        default_factory=lambda: ["discrete_classification"]
    )
    LAND_COVER_IMAGE_RES = 100
    LAND_COVER_PERIOD = 1500
    BUCKET_NAME = "earthengine-bucket"
    PATH_TO_PRIVATE_KEY = (
        "" # please provide the path to the private key for the service account
    )
    BUCKET_NAME = "" # please provide the bucket name
    SERVICE_ACCOUNT = "" # please provide the service account

    @property
    def ALL_SATELLITES(self) -> zip(List[str], List[str]): # type: ignore
        """Return varying satellites to be fed into the model"""
        return zip(
            [
                self.AOD_IMAGE_COLLECTION,
                self.LANDSAT_IMAGE_COLLECTION,
                self.NIGHTTIME_LIGHT_IMAGE_COLLECTION,
                self.METEROLOGICAL_IMAGE_COLLECTION,
                self.POPULATION_IMAGE_COLLECTION,
                self.LAND_COVER_IMAGE_COLLECTION,
            ],
            [
                self.AOD_IMAGE_BAND,
                self.LANDSAT_IMAGE_BAND,
                self.NIGHTTIME_LIGHT_IMAGE_BAND,
                self.METEROLOGICAL_IMAGE_BAND,
                self.POPULATION_IMAGE_BAND,
                self.LAND_COVER_IMAGE_BAND,
            ],
            [
                self.AOD_IMAGE_PERIOD,
                self.LANDSAT_PERIOD,
                self.NIGHTTIME_LIGHT_PERIOD,
                self.METEROLOGICAL_IMAGE_PERIOD,
                self.POPULATION_PERIOD,
                self.LAND_COVER_PERIOD,
            ],
            [
                self.AOD_IMAGE_RES,
                self.LANDSAT_RES,
                self.NIGHTTIME_LIGHT_RES,
                self.METEROLOGICAL_IMAGE_RES,
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
    CITY = ""  
    SENSOR_TYPE = "reference grade"
    REGION = "us-east-1"
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    TABLE_NAME = ""
    SCHEMA_NAME: str = ""
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_pollutant=["parameter"],
            filter_non_null_values=["value"],
            filter_extreme_values=["value"],
            filter_no_coordinates=["coordinates"],
            filter_countries=["country"],
            filter_cities=["city"],
        ),
    )
    TARGET_VARIABLE = ""
    COUNTRY = ""
    SOURCE = ""
    LOCAL_DATA = ""


@dataclass
class TimeSplitterConfig:
    DATE_COL: str = "date.utc"
    TARGET_VARIABLE = ""
    COUNTRY = ""
    CITY = ""
    SENSOR_TYPE = "reference grade"
    SOURCE = ""
    LOCAL_DATA = ""

    TIME_WINDOW_LENGTH: int = 4
    WITHIN_WINDOW_SAMPLER: int = 4
    WINDOW_COUNT: int = 10  # this will increase for more than one split
    TABLE_NAME: str = ""
    REGION = ""
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

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
    PLOTS_TABLE_NAME: str = "plots"
    PLOTS_SCHEMA_NAME: str = "model_output"
    RESULTS_TABLE_NAME: str = "results"


@dataclass
class MatrixGeneratorConfig:
    ALGORITHM = "RFR"
    ID_COLUMN_LIST: Sequence[str] = field(
        default_factory=lambda: ["locationId", "cohort", "cohort_type"]
    )
    # Satellite configurations
    SATELLITE_CONFIG = {
        "MODIS/061/MCD19A2_GRANULES": {
            "bands": ["Optical_Depth_047"],
            "resolution": 1000,
            "time_ranges": [("00:00:00", "08:00:00")],
            "frequency": "daily",
        },
        "LANDSAT/LC08/C02/T1_L2": {
            "bands": ["SR_B4", "SR_B3", "SR_B2"],
            "resolution": 30,
            "time_ranges": [("03:30:00", "04:00:00")],
            "frequency": "weekly",
        },
        "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG": {
            "bands": ["avg_rad"],
            "resolution": 463.83,
            "time_ranges": [("00:00:00", "00:59:59")],
            "frequency": "monthly",
        },
        "NOAA/GFS0P25": {
            "bands": [
                "temperature_2m_above_ground",
                "relative_humidity_2m_above_ground",
                "precipitable_water_entire_atmosphere",
                "total_cloud_cover_entire_atmosphere",
                "u_component_of_wind_10m_above_ground",
                "v_component_of_wind_10m_above_ground",
            ],
            "resolution": 27830,
            "time_ranges": [
                ("00:00:00", "00:59:59"),
                ("06:00:00", "06:59:59"),
                ("12:00:00", "12:59:59"),
                ("18:00:00", "18:59:59"),
            ],
            "frequency": "daily",
        },
    }


@dataclass
class FeatureImportanceConfig:
    NUM_RECORDS: int = 5
    TABLE_NAME: str = "feature_importance"


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
    All_MODEL_FEATURES = [
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
        "basic_demographic_characteristics",
        "discrete_classification",
    ]


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
    TABLE_NAME = ""
    TARGET_COL: str = "value"
    TARGET_VARIABLE = "pm25"
    COUNTRY = ""
    CITY = ""
    CATEGORICAL_FEATURES: List[str] = field(
        default_factory=lambda: ["locationId"]
    )
    CORE_FEATURES: List[str] = field(default_factory=list)
    SATELLITE_FEATURES: List[StrictStr] = field(
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
            set(
                self.CORE_FEATURES
                + self.CATEGORICAL_FEATURES
                + self.SATELLITE_FEATURES
            )
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
    TARGET_VARIABLE = "pm25"
    COUNTRY = ""
    SOURCE = "openaq-aws"
    LOCAL_DATA = ""


@dataclass
class EEConfig:
    LOOKBACK_N = 1
    DATE_COL: str = "timestamp_utc"
    TABLE_NAME = "cohorts"
    BUCKET_NAME = ""
    PATH_TO_PRIVATE_KEY = ""
    SERVICE_ACCOUNT = ""
    ALL_SATELLITES = {
        "MODIS/061/MCD19A2_GRANULES": {
            "bands": ["Optical_Depth_047"],
            "resolution": 1000,
            "frequency": "daily",
        },
        "LANDSAT/LC08/C02/T1_L2": {
            "bands": ["SR_B4", "SR_B3", "SR_B2"],
            "resolution": 30,
            "frequency": "weekly",
        },
        "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG": {
            "bands": ["avg_rad"],
            "resolution": 463.83,
            "frequency": "monthly",
        },
        "NOAA/GFS0P25": {
            "bands": [
                "temperature_2m_above_ground",
                "relative_humidity_2m_above_ground",
                "precipitable_water_entire_atmosphere",
                "u_component_of_wind_10m_above_ground",
                "v_component_of_wind_10m_above_ground",
            ],
            "resolution": 27830,
            "frequency": "daily",
        },
    }


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

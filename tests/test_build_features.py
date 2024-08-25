import datetime
import os
import unittest.mock as mock
from unittest.mock import MagicMock

import pandas as pd
import pytest
from src.features.build_features import (
    BuildFeaturesRandomForest,
    get_feature_builder,
)
from src.features.satellite._ee_data import EEFeatures

from config.model_settings import BuildFeaturesConfig, EEConfig


@pytest.fixture
def mock_engine():
    return MagicMock()


# Mock environment variables for Earth Engine credentials
@pytest.fixture
def mock_env_vars():
    with mock.patch.dict(
        os.environ,
        {
            "SERVICE_ACCOUNT_EMAIL": "fake_service_account_email@example.com",
            "EARTHENGINE_CREDENTIALS": "/fake/path/to/private_key.json",
        },
    ):
        yield


@pytest.fixture
def cohort_df():
    return pd.DataFrame(
        {
            "locationId": [1, 2, 3, 4],
            "cohort": ["A", "A", "B", "B"],
            "timestamp_utc": [
                "2021-01-01",
                "2021-01-02",
                "2021-01-01",
                "2021-01-02",
            ],
            "cohort_type": [
                "training",
                "training",
                "validation",
                "validation",
            ],
            "value": [10, 20, 30, 40],
        }
    )


@pytest.fixture
def feature_df():
    return pd.DataFrame(
        {
            "location_id": [1, 2, 3, 4],
            "cohort": ["A", "A", "B", "B"],
            "timestamp_utc": [
                "2021-01-01",
                "2021-01-02",
                "2021-01-01",
                "2021-01-02",
            ],
        }
    )


def test_build_features_random_forest_initialization():
    config = BuildFeaturesConfig(
        CATEGORICAL_FEATURES=[],
        ALL_MODEL_FEATURES=[],
        TARGET_COL="value",
    )
    builder = BuildFeaturesRandomForest.from_dataclass_config(config)
    assert builder.categorical_features == config.CATEGORICAL_FEATURES
    assert builder.all_model_features == config.ALL_MODEL_FEATURES
    assert builder.target_col == config.TARGET_COL


def test_add_ee_features(feature_df):
    # Mock the environment variables
    with mock.patch.dict(
        os.environ,
        {
            "SERVICE_ACCOUNT_EMAIL": "fake_service_account_email@example.com",
            "EARTHENGINE_CREDENTIALS": "/fake/path/to/private_key.json",
        },
    ):
        config = BuildFeaturesConfig(
            CATEGORICAL_FEATURES=[],
            ALL_MODEL_FEATURES=[],
            TARGET_COL="value",
        )
        # Define the required arguments for _add_ee_features
        x = -70.214134  # Example longitude, replace with actual value
        y = 44.089355  # Example latitude, replace with actual value
        table_name = "example_table"
        # Create an instance of BuildFeaturesRandomForest and call _add_ee_features
        builder = BuildFeaturesRandomForest.from_dataclass_config(config)
        # Mock EEFeatures.execute method to return a predefined DataFrame
        with mock.patch.object(EEFeatures, "execute", return_value=feature_df):
            result = builder._add_ee_features(x, y, table_name)

        # Assert that the resulting DataFrame equals the original feature_df
        assert result.equals(feature_df)


def test_split_train_valid(cohort_df, feature_df):
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    result = builder._split_train_valid(cohort_df, feature_df)
    assert isinstance(result, tuple)
    assert len(result) == 6


def test_get_feature_builder():
    builder_class = get_feature_builder("RFC")
    assert builder_class == BuildFeaturesRandomForest

    with pytest.raises(ValueError):
        get_feature_builder("non_existent")


def test_all_model_features_property():
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    assert builder.all_model_features == ["col1", "col2"]

    with pytest.raises(ValueError):
        builder.all_model_features = ["col1", 2]


def test_add_year():
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    df = pd.DataFrame({"day": ["2021-01-01", "2021-01-02"]})
    result = builder._add_year(df)
    assert "year" in result.columns
    assert list(result["year"]) == [2021, 2021]


def test_ee_features_initialization(mock_env_vars):
    config = EEConfig(
        DATE_COL="timestamp_utc",
        BUCKET_NAME="fake-bucket",
        LOOKBACK_N=3,
    )
    ee_features = EEFeatures.from_dataclass_config(config)
    assert ee_features.date_col == config.DATE_COL
    assert ee_features.bucket_name == config.BUCKET_NAME
    assert ee_features.lookback_n == config.LOOKBACK_N


def test_ee_features_execute(mock_env_vars, feature_df):
    ee_features = EEFeatures(
        satellite_config={
            "LANDSAT/LC08/C02/T1_L2": {
                "bands": ["SR_B4", "SR_B3", "SR_B2"],
                "frequency": "daily",
                "resolution": 30,
                "time_ranges": [("00:00:00", "23:59:59")],
            }
        },
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    # Mock the Earth Engine initialization
    with mock.patch("ee.Initialize"), mock.patch(
        "ee.ServiceAccountCredentials"
    ):
        # Mock the internal query method
        with mock.patch.object(
            ee_features,
            "query_satellite_for_time_range",
            return_value=[feature_df],
        ):
            # Disable parallel processing during the test
            result = ee_features.execute(
                -70.214134,
                44.089355,
                "example_table",
                False,
                use_parallel=False,
            )

    # Assertions
    assert result.equals(feature_df)


# Test the query_satellite method
def test_ee_features_query_satellite(mock_env_vars):
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    mock_image_collection = MagicMock()
    with mock.patch.object(
        ee_features,
        "execute_for_collection",
        return_value=mock_image_collection,
    ), mock.patch.object(
        ee_features,
        "get_satellite_data",
        return_value=pd.DataFrame({"SR_B4": [1.0]}),
    ), mock.patch(
        "src.features.satellite._ee_data.get_dbengine"
    ), mock.patch(
        "src.features.satellite._ee_data.write_to_db"
    ):
        result = ee_features.query_satellite(
            "LANDSAT/LC08/C02/T1_L2",
            ["SR_B4", "SR_B3", "SR_B2"],
            datetime.datetime(2021, 1, 1, 0, 0),
            datetime.datetime(2021, 1, 1, 23, 59),
            -70.214134,
            44.089355,
            30,
            False,
        )
        assert isinstance(result, pd.DataFrame)
        assert not result.empty


def test_create_satellite_dataframe():
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    # Adjusted 'info' to include 'longitude', 'latitude', and 'time' columns
    info = [
        ["longitude", "latitude", "time", "SR_B4", "SR_B3", "SR_B2"],
        [106.79481, 47.922497, 1, 1.0, 2.0, 3.0],
        [106.79481, 47.922497, 2, 4.0, 5.0, 6.0],
    ]

    result = ee_features._create_satellite_dataframe(
        info, ["SR_B4", "SR_B3", "SR_B2"], "2021-01-01", -70.214134, 44.089355
    )

    # Assertions to ensure the DataFrame was created correctly
    assert "SR_B4" in result.columns
    assert "SR_B3" in result.columns
    assert "SR_B2" in result.columns
    assert "longitude" in result.columns
    assert "latitude" in result.columns
    assert "time" in result.columns
    assert not result.empty


def test_calculate_spatial_weighted_average():
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    df = pd.DataFrame(
        {
            "latitude": [44.089355, 44.089355],
            "longitude": [-70.214134, -70.214134],
        }
    )

    result = ee_features._calculate_spatial_weighted_average(
        df, -70.214134, 44.089355
    )
    assert "distance" in result.columns


# Test the bands_available method
def test_bands_available():
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    mock_image_collection = MagicMock()
    mock_image_collection.first.return_value.select.return_value.getInfo.return_value = {
        "bands": ["SR_B4", "SR_B3", "SR_B2"]
    }
    result = ee_features.bands_available(
        mock_image_collection, ["SR_B4", "SR_B3"]
    )
    assert result is True


# Test the _generate_timerange method
def test_generate_timerange(mock_env_vars):
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    with mock.patch(
        "src.features.satellite._ee_data.get_data",
        return_value=pd.DataFrame({"datetime": ["2021-01-01"]}),
    ):
        end_date, start_date = ee_features._generate_timerange("example_table")
        assert end_date == "2021-01-01"
        assert start_date == "2021-01-01"


def test_get_satellite_data_within_hour(mock_env_vars):
    ee_features = EEFeatures(
        satellite_config={},
        date_col="timestamp_utc",
        bucket_name="fake-bucket",
        lookback_n=3,
    )

    mock_image_collection = MagicMock()
    mock_image_collection.filterDate.return_value = mock_image_collection

    with mock.patch(
        "ee.ServiceAccountCredentials"
    ) as mock_credentials, mock.patch("ee.Initialize"), mock.patch(
        "ee.Date", return_value=MagicMock()
    ), mock.patch.object(
        ee_features, "bands_available", return_value=True
    ), mock.patch.object(
        ee_features,
        "_create_satellite_dataframe",
        return_value=pd.DataFrame({"SR_B4": [1.0]}),
    ):

        # Mock the credentials initialization
        mock_credentials.return_value = MagicMock()

        # Call the function
        result = ee_features.get_satellite_data_within_hour(
            mock_image_collection,
            ["SR_B4", "SR_B3", "SR_B2"],
            datetime.datetime(2021, 1, 1, 0, 0),
            datetime.datetime(2021, 1, 1, 23, 59),
            -70.214134,
            44.089355,
            30,
        )
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

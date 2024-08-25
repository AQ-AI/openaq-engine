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

from config.model_settings import BuildFeaturesConfig


@pytest.fixture
def mock_engine():
    return MagicMock()


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

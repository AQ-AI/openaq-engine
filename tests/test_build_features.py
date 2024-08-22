from unittest.mock import MagicMock

import pandas as pd
import pytest

from config.model_settings import BuildFeaturesConfig
from openaq_engine.src.features.build_features import (
    BuildFeaturesRandomForest,
    get_feature_builder,
)


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
    satellite_config = {"some_key": "some_value"}
    builder = BuildFeaturesRandomForest.from_dataclass_config(
        satellite_config, config
    )
    assert builder.categorical_features == config.CATEGORICAL_FEATURES
    assert builder.all_model_features == config.ALL_MODEL_FEATURES
    assert builder.target_col == config.TARGET_COL


def test_add_ee_features(feature_df):
    # Create an instance of BuildFeaturesRandomForest and call _add_ee_features
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    result = builder._add_ee_features(feature_df)

    # Assert that the resulting DataFrame equals the original feature_df
    assert result.equals(feature_df)


def test_split_train_valid(cohort_df, feature_df):
    satellite_config = {"some_key": "some_value"}
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
        satellite_config=satellite_config,
    )
    result = builder._split_train_valid(cohort_df, feature_df)
    assert isinstance(result, tuple)
    assert len(result) == 6


def test_results_to_db(mocker, mock_engine, feature_df):
    mock_write_to_db = mocker.patch(
        "openaq_engine.src.features.build_features.write_to_db"
    )
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    builder._results_to_db(feature_df, mock_engine)
    mock_write_to_db.assert_called_once_with(
        feature_df,
        mock_engine,
        "features",
        "public",
        "append",
    )


def test_get_feature_builder():
    builder_class = get_feature_builder("RFC")
    assert builder_class == BuildFeaturesRandomForest

    with pytest.raises(ValueError):
        get_feature_builder("non_existent")


def test_all_model_features_property():
    satellite_config = {"some_key": "some_value"}
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
        satellite_config=satellite_config,
    )
    assert builder.all_model_features == ["col1", "col2"]

    with pytest.raises(ValueError):
        builder.all_model_features = ["col1", 2]


def test_add_year():
    satellite_config = {"some_key": "some_value"}
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
        satellite_config=satellite_config,
    )
    df = pd.DataFrame({"day": ["2021-01-01", "2021-01-02"]})
    result = builder._add_year(df)
    assert "year" in result.columns
    assert list(result["year"]) == [2021, 2021]

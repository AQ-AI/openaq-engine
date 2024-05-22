from unittest.mock import MagicMock

import pandas as pd
import pytest
from src.features.build_features import (
    BuildFeaturesRandomForest,
    get_feature_builder,
)

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
        CATEGORICAL_FEATURES=["col1", "col2"],
        ALL_MODEL_FEATURES=["col1", "col2"],
        TARGET_COL="value",
    )
    builder = BuildFeaturesRandomForest.from_dataclass_config(config)
    assert builder.categorical_features == config.CATEGORICAL_FEATURES
    assert builder.all_model_features == config.ALL_MODEL_FEATURES
    assert builder.target_col == config.TARGET_COL


def test_add_ee_features(mocker, feature_df):
    mock_ee_features = mocker.patch(
        "src.features.satellite._ee_data.EEFeatures.from_dataclass_config"
    )
    mock_ee_instance = mock_ee_features.return_value
    mock_ee_instance.execute.return_value = feature_df

    builder = BuildFeaturesRandomForest(
        categorical_features=["location_id", "cohort"],
        all_model_features=["location_id", "cohort"],
    )
    x, y, timestamp_hour = 1, 2, "2021-01-01T00:00:00Z"
    result = builder._add_ee_features(x, y, timestamp_hour)
    assert result.equals(feature_df)

    # Ensure the mocked EEFeatures was called correctly
    mock_ee_features.assert_called_once()
    mock_ee_instance.execute.assert_called_once_with(
        x, y, timestamp_hour, save_images=False
    )


def test_split_train_valid(cohort_df, feature_df):
    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    result = builder._split_train_valid(cohort_df, feature_df)
    assert isinstance(result, tuple)
    assert len(result) == 6


def test_execute(mocker):
    mock_engine = MagicMock()

    feature_df = pd.DataFrame({"col1": [1], "col2": [2]})

    mocker.patch.object(
        BuildFeaturesRandomForest, "_add_ee_features", return_value=feature_df
    )
    mocker.patch.object(
        BuildFeaturesRandomForest,
        "_change_to_categorical_type",
        return_value=feature_df,
    )

    builder = BuildFeaturesRandomForest(
        categorical_features=["col1", "col2"],
        all_model_features=["col1", "col2"],
    )
    result = builder.execute(mock_engine, 1, 2, "2020-01-01T00:00:00Z")
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

    # Ensure the mocked methods were called
    builder._add_ee_features.assert_called_once_with(
        1, 2, "2020-01-01T00:00:00Z"
    )
    builder._change_to_categorical_type.assert_called_once_with(feature_df)


def test_results_to_db(mocker, mock_engine, feature_df):
    mock_write_to_db = mocker.patch("src.features.build_features.write_to_db")
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

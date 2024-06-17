import datetime
import os
import tempfile

import joblib
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

from src.features.build_features import BuildFeaturesRandomForest
from src.matrix_generator import MatrixGenerator

from config.model_settings import MatrixGeneratorConfig


# Fixtures
@pytest.fixture
def config():
    return MatrixGeneratorConfig()


@pytest.fixture
def matrix_generator(config):
    return MatrixGenerator(
        algorithm="RFR",
        id_column_list=config.ID_COLUMN_LIST,
        satellite_config=config.SATELLITE_CONFIG,
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


def test_get_feature_generator():
    config = MatrixGeneratorConfig()
    matrix_generator = MatrixGenerator(
        algorithm="RFR",
        id_column_list=config.ID_COLUMN_LIST,
        satellite_config=config.SATELLITE_CONFIG,
    )
    feature_generator = matrix_generator._get_feature_generator(
        matrix_generator.satellite_config
    )
    assert isinstance(feature_generator, BuildFeaturesRandomForest)


def test_get_feature_generator_invalid():
    config = MatrixGeneratorConfig()
    matrix_generator = MatrixGenerator(
        algorithm="INVALID",  # Set an invalid algorithm here
        id_column_list=config.ID_COLUMN_LIST,
        satellite_config=config.SATELLITE_CONFIG,
    )
    with pytest.raises(ValueError):
        matrix_generator._get_feature_generator(
            matrix_generator.satellite_config
        )  # No arguments needed


def test_execute(matrix_generator, mock_engine):
    with patch.object(
        matrix_generator,
        "matrix_generator",
        return_value=pd.DataFrame({"col": [1, 2, 3]}),
    ) as mock_method:
        df = matrix_generator.execute(mock_engine, 1, 2, "test_table")
        assert not df.empty
        mock_method.assert_called_once_with(mock_engine, 1, 2, "test_table")


def test_matrix_generator_method(matrix_generator, mock_engine):
    with patch.object(
        BuildFeaturesRandomForest,
        "execute",
        return_value=pd.DataFrame({"col": [1, 2, 3]}),
    ) as mock_method:
        df = matrix_generator.matrix_generator(mock_engine, 1, 2, "test_table")
        assert not df.empty
        mock_method.assert_called_once_with(mock_engine, 1, 2, "test_table")


def test_combine_tv_sets(matrix_generator):
    data = [
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "SR_B2": [13539],
                "SR_B3": [12604],
                "SR_B4": [11023],
                "Optical_Depth_047": [0.174],
                "tv_set": [[1]],
            }
        ),
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "SR_B2": [13539],
                "SR_B3": [12604],
                "SR_B4": [11023],
                "Optical_Depth_047": [0.174],
                "tv_set": [[2]],
            }
        ),
    ]
    combined_df = matrix_generator.combine_tv_sets(data)
    assert not combined_df.empty
    assert combined_df["tv_set"].iloc[0] == [1, 2]


def test_extract_time_ranges(matrix_generator):
    # Ensure the correct environment variables are used
    os.environ["TEST_PGDATABASE"] = "test_db"
    os.environ["TEST_PGUSER"] = "test_user"
    os.environ["TEST_PGPASSWORD"] = "test_password"
    os.environ["TEST_PGHOST"] = "localhost"
    os.environ["PGPORT"] = "5432"

    data = [
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "SR_B2": [13539],
                "SR_B3": [12604],
                "SR_B4": [11023],
                "Optical_Depth_047": [0.174],
                "tv_set": [[1]],
            }
        ),
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "SR_B2": [13539],
                "SR_B3": [12604],
                "SR_B4": [11023],
                "Optical_Depth_047": [0.174],
                "tv_set": [[2]],
            }
        ),
    ]
    combined_df = matrix_generator.combine_tv_sets(data)
    assert not combined_df.empty
    assert combined_df["tv_set"].iloc[0] == [1, 2]

    time_ranges = matrix_generator.extract_time_ranges("cohorts_mumbai")
    assert time_ranges is not None


def test_query_satellite_data(matrix_generator):
    cohort_data = pd.DataFrame(
        {
            "x": [-70.214134],
            "y": [44.089355],
            "value": [10],
            "datetime_hour": pd.to_datetime(["2022-04-01 21:00:00"]),
        }
    )

    satellite_data = pd.DataFrame(
        {
            "sensor_longitude": [-70.214134],
            "sensor_latitude": [44.089355],
            "datetime_hour": pd.to_datetime(["2022-04-01 21:00:00"]),
            "SR_B2": [13539],
            "SR_B3": [12604],
            "SR_B4": [11023],
            "Optical_Depth_047": [0.174],
        }
    )

    with patch(
        "src.utils.utils.get_data", side_effect=[cohort_data, satellite_data]
    ):
        result_df = matrix_generator.query_satellite_data(
            tv_id=0,
            x=-70.214134,
            y=44.089355,
            start_date="2022-04-01T21:00:00.000000Z",
            end_date="2023-04-01T21:00:00.000000Z",
            cohort_table="cohorts_mumbai",
        )

        assert not result_df.empty
        assert "value" in result_df.columns


def test_get_csr(mocker):
    config = MatrixGeneratorConfig()
    matrix_generator = MatrixGenerator(
        algorithm="RFR",
        id_column_list=config.ID_COLUMN_LIST,
        satellite_config=config.SATELLITE_CONFIG,
    )
    mock_data = {"mock": "data"}

    # Create a temporary file to store the joblib data
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".joblib"
    ) as tmp_file:
        joblib.dump(mock_data, tmp_file)
        tmp_file_path = tmp_file.name

    with patch("joblib.load", return_value=mock_data):
        with patch("os.path.join", return_value=tmp_file_path):
            result = matrix_generator._get_csr(
                0, "training", datetime.date(2020, 1, 1)
            )

    assert result == [mock_data]

    # Clean up the temporary file
    os.remove(tmp_file_path)

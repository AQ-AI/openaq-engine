import datetime
import os
import tempfile
from unittest.mock import MagicMock, patch

import joblib
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from config.model_settings import MatrixGeneratorConfig
from openaq_engine.src.features.build_features import BuildFeaturesRandomForest
from openaq_engine.src.matrix_generator import MatrixGenerator


@pytest.fixture
def config():
    # Create a custom configuration with a single satellite for testing
    config = MatrixGeneratorConfig()
    config.SATELLITE_CONFIG = {
        "modis_061_mcd19a2_granules": {
            "bands": ["optical_depth_047", "sr_b2", "sr_b3", "sr_b4"],
            "frequency": "daily",
        }
    }
    return config


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
            "cohort": [
                "A_2021-01-01_2021-01-02",
                "A_2021-01-02_2021-01-03",
                "B_2021-01-01_2021-01-02",
                "B_2021-01-02_2021-01-03",
            ],
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
        satellite_config=config.SATELLITE_CONFIG,
        algorithm="RFR",
        id_column_list=config.ID_COLUMN_LIST,
    )
    feature_generator = matrix_generator._get_feature_generator()
    assert isinstance(feature_generator, BuildFeaturesRandomForest)


def test_get_feature_generator_invalid():
    config = MatrixGeneratorConfig()
    matrix_generator = MatrixGenerator(
        satellite_config=config.SATELLITE_CONFIG,
        algorithm="INVALID",  # Set an invalid algorithm here
        id_column_list=config.ID_COLUMN_LIST,
    )
    with pytest.raises(ValueError):
        matrix_generator._get_feature_generator()


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
                "sr_b2": [13539],
                "sr_b3": [12604],
                "sr_b4": [11023],
                "optical_depth_047": [0.174],
                "tv_set": [[1]],
            }
        ),
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "sr_b2": [13539],
                "sr_b3": [12604],
                "sr_b4": [11023],
                "optical_depth_047": [0.174],
                "tv_set": [[2]],
            }
        ),
    ]
    combined_df = matrix_generator.combine_tv_sets(data)
    assert not combined_df.empty
    assert combined_df["tv_set"].iloc[0] == [1, 2]


def test_extract_time_ranges(matrix_generator):
    data = [
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "sr_b2": [13539],
                "sr_b3": [12604],
                "sr_b4": [11023],
                "optical_depth_047": [0.174],
                "tv_set": [[1]],
            }
        ),
        pd.DataFrame(
            {
                "sensor_longitude": [106.79481],
                "sensor_latitude": [47.922497],
                "datetime_hour": ["2020-05-07 03:00:00"],
                "sr_b2": [13539],
                "sr_b3": [12604],
                "sr_b4": [11023],
                "optical_depth_047": [0.174],
                "tv_set": [[2]],
            }
        ),
    ]
    combined_df = matrix_generator.combine_tv_sets(data)
    assert not combined_df.empty
    assert combined_df["tv_set"].iloc[0] == [1, 2]

    time_ranges = matrix_generator.extract_time_ranges("cohorts_mumbai")
    assert time_ranges is not None


def get_test_engine():
    db_url = f"postgresql://{os.getenv('TEST_PGUSER')}:{os.getenv('TEST_PGPASSWORD')}@{os.getenv('TEST_PGHOST')}:{os.getenv('TEST_PGPORT')}/{os.getenv('TEST_PGDATABASE')}"
    print(f"Connecting to: {db_url}")
    return create_engine(db_url)


def check_table_exists(engine, table_name):
    with engine.connect() as connection:
        result = connection.execute(
            text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
            )
        )
        exists = result.scalar()
        print(f"Table '{table_name}' exists: {exists}")
        return exists


def test_query_satellite_data(matrix_generator):
    # Debug: Check database connection and table existence
    engine = get_test_engine()
    table_exists = check_table_exists(engine, "cohorts_mumbai")
    assert (
        table_exists
    ), "Table 'cohorts_mumbai' does not exist in the test database."

    # Mocking the get_data function to return predefined DataFrame for testing
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
            "sr_b2": [13539],
            "sr_b3": [12604],
            "sr_b4": [11023],
            "optical_depth_047": [0.174],
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

        # Debug: Check the contents of result_df
        print("Result DataFrame:")
        print(result_df)

        assert not result_df.empty, "Result DataFrame is empty."
        assert "value" in result_df.columns
        assert "optical_depth_047" in result_df.columns
        assert "sr_b2" in result_df.columns
        assert "sr_b3" in result_df.columns
        assert "sr_b4" in result_df.columns


def test_get_csr(mocker):
    config = MatrixGeneratorConfig()
    matrix_generator = MatrixGenerator(
        algorithm="RFR",
        id_column_list=config.ID_COLUMN_LIST,
        satellite_config=config.SATELLITE_CONFIG,
    )
    mock_data = [
        {"mock": "data"}
    ]  # Adjusted to match the expected result format

    # Create a temporary file to store the joblib data
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".joblib"
    ) as tmp_file:
        joblib.dump(mock_data, tmp_file)
        tmp_file_path = tmp_file.name

    with patch("joblib.load", return_value=mock_data):
        with patch("os.path.join", return_value=tmp_file_path):
            result = matrix_generator._get_csr(
                0, "training", datetime.datetime(2020, 1, 1)
            )

    assert result == mock_data
    # Clean up the temporary file
    os.remove(tmp_file_path)

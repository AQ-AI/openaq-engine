from unittest.mock import patch

import pandas as pd
import pytest
from shapely.geometry import Point

from openaq_engine.src.preprocess import Preprocess


@pytest.fixture
def sample_data():
    data = {
        "date": [
            '{"utc": "2022-04-01T21:00:00Z", "local": "2022-04-01T14:00:00-07:00"}',
            '{"utc": "2022-04-02T21:00:00Z", "local": "2022-04-02T14:00:00-07:00"}',
        ],
        "coordinates": [
            '{"latitude": 37.7749, "longitude": -122.4194}',
            '{"latitude": 34.0522, "longitude": -118.2437}',
        ],
        "value": [10, 20],
        "parameter": ["pm25", "pm25"],
        "country": ["US", "US"],  # Corrected column name
        "city": ["San Francisco", "Los Angeles"],  # Add city column
        "pnt": [Point(37.7749, -122.4194), Point(34.0522, -118.2437)],
    }
    return pd.DataFrame(data)


def test_extract_timestamp_from_api():
    preprocess = Preprocess()
    data = {
        "date": [
            '{"utc": "2022-04-01T21:00:00Z", "local": "2022-04-01T14:00:00-07:00"}'
        ],
        "coordinates": ['{"latitude": 37.7749, "longitude": -122.4194}'],
        "value": [10],
        "pnt": [Point(37.7749, -122.4194)],
    }
    df = pd.DataFrame(data)
    result = df.apply(preprocess._extract_timestamp_from_api, axis=1)

    expected_utc = "2022-04-01T21:00:00.000000Z"
    expected_local = "2022-04-01T14:00:00.000000-0700"

    assert result["timestamp_utc"].iloc[0] == expected_utc
    assert result["timestamp_local"].iloc[0] == expected_local


def test_get_timestamps_aws(sample_data):
    preprocess = Preprocess()
    sample_data["date"] = [
        "{utc=2022-04-01T21:00:00, local=2022-04-01T14:00:00-07:00}",
        "{utc=2022-04-02T21:00:00, local=2022-04-02T14:00:00-07:00}",
    ]
    result = preprocess.get_timestamps(sample_data, "openaq-aws")
    assert "timestamp_utc" in result.columns
    assert "timestamp_local" in result.columns
    assert result["timestamp_utc"].iloc[0] == "2022-04-01T21:00:00.000000Z"
    assert result["timestamp_local"].iloc[0] == "2022-04-01T14:00:00-0700"


def test_extract_coordinates_api(sample_data):
    preprocess = Preprocess()
    result = preprocess.extract_coordinates(sample_data, "openaq-api")
    assert "x" in result.columns
    assert "y" in result.columns
    assert result["x"].iloc[0] == -122.4194
    assert result["y"].iloc[0] == 37.7749


def test_extract_coordinates_aws(sample_data):
    preprocess = Preprocess()
    sample_data["coordinates"] = [
        "{latitude=37.7749, longitude=-122.4194}",
        "{latitude=34.0522, longitude=-118.2437}",
    ]
    result = preprocess.extract_coordinates(sample_data, "openaq-aws")
    assert "x" in result.columns
    assert "y" in result.columns
    assert result["x"].iloc[0] == -122.4194
    assert result["y"].iloc[0] == 37.7749


def test_validate_point(sample_data):
    preprocess = Preprocess()
    result = preprocess.validate_point(sample_data)
    assert not result.empty


def test_validate_point_with_invalid_points():
    preprocess = Preprocess()
    data = {"pnt": [Point(37.7749, -122.4194), Point()]}
    df = pd.DataFrame(data)
    result = preprocess.validate_point(df)
    assert len(result) == 1


def test_dict_cols_to_json():
    preprocess = Preprocess()
    df = pd.DataFrame(
        {
            "col1": [{"key1": "value1"}, {"key2": "value2"}],
            "col2": [1, 2],
        }
    )
    result = preprocess.dict_cols_to_json(df)
    assert result["col1"].apply(lambda x: isinstance(x, str)).all()


def test_dict_cols_to_json_with_non_dict_values():
    preprocess = Preprocess()
    df = pd.DataFrame(
        {
            "col1": [{"key1": "value1"}, "not a dict"],
            "col2": [1, 2],
        }
    )
    result = preprocess.dict_cols_to_json(df)
    assert isinstance(result["col1"].iloc[0], str)
    assert result["col1"].iloc[1] == "not a dict"


def test_filter_data(sample_data):
    preprocess = Preprocess(
        filter_pollutant=False,
        filter_non_null_values=False,
        filter_extreme_values=False,
        filter_no_coordinates=False,
        filter_countries=False,
        filter_cities=False,
    )
    with patch(
        "src.preprocessing.filter.Filter.filter_pollutant",
        return_value=sample_data,
    ):
        with patch(
            "src.preprocessing.filter.Filter.filter_no_coordinates",
            return_value=sample_data,
        ):
            with patch(
                "src.preprocessing.filter.Filter.filter_extreme_values",
                return_value=sample_data,
            ):
                with patch(
                    "src.preprocessing.filter.Filter.filter_non_null_values",
                    return_value=sample_data,
                ):
                    with patch(
                        "src.preprocessing.filter.Filter.filter_countries",
                        return_value=sample_data,
                    ):
                        with patch(
                            "src.preprocessing.filter.Filter.filter_cities",
                            return_value=sample_data,
                        ):
                            result = preprocess.filter_data(sample_data)
                            assert not result.empty


def test_filter_data_with_filters(sample_data):
    preprocess = Preprocess(
        filter_pollutant=True,
        filter_non_null_values=True,
        filter_extreme_values=True,
        filter_no_coordinates=True,
        filter_countries=True,
        filter_cities=True,
    )
    mock_countries = ["US", "GB"]  # Example countries list
    mock_cities = ["San Francisco", "Los Angeles"]  # Example cities list

    with patch(
        "src.preprocessing.filter.Filter.filter_pollutant",
        return_value=sample_data,
    ):
        with patch(
            "src.preprocessing.filter.Filter.filter_no_coordinates",
            return_value=sample_data,
        ):
            with patch(
                "src.preprocessing.filter.Filter.filter_extreme_values",
                return_value=sample_data,
            ):
                with patch(
                    "src.preprocessing.filter.Filter.filter_non_null_values",
                    return_value=sample_data,
                ):
                    with patch(
                        "src.preprocessing.filter.Filter.filter_countries",
                        return_value=sample_data,
                    ) as mock_filter_countries:
                        with patch(
                            "src.preprocessing.filter.Filter.filter_cities",
                            return_value=sample_data,
                        ) as mock_filter_cities:
                            # Run the filter_data method
                            result = preprocess.filter_data(sample_data)
                            assert not result.empty

                            # Ensure filter_countries was called with the correct arguments
                            mock_filter_countries.assert_called_once_with(
                                sample_data, countries=mock_countries
                            )

                            # Ensure filter_cities was called with the correct arguments
                            mock_filter_cities.assert_called_once_with(
                                sample_data, cities=mock_cities
                            )


def test_execute(sample_data):
    preprocess = Preprocess()
    with patch.object(preprocess, "get_timestamps", return_value=sample_data):
        with patch.object(
            preprocess, "extract_coordinates", return_value=sample_data
        ):
            with patch.object(
                preprocess, "filter_data", return_value=sample_data
            ):
                with patch.object(
                    preprocess, "validate_point", return_value=sample_data
                ):
                    with patch.object(
                        preprocess,
                        "dict_cols_to_json",
                        return_value=sample_data,
                    ):
                        result = preprocess.execute(sample_data, "openaq-api")
                        assert not result.empty


def test_execute_with_no_valid_points():
    preprocess = Preprocess()
    empty_data = pd.DataFrame(columns=["date", "coordinates", "value", "pnt"])
    with patch.object(preprocess, "get_timestamps", return_value=empty_data):
        with patch.object(
            preprocess, "extract_coordinates", return_value=empty_data
        ):
            with patch.object(
                preprocess, "filter_data", return_value=empty_data
            ):
                with patch.object(
                    preprocess, "validate_point", return_value=empty_data
                ):
                    with patch.object(
                        preprocess,
                        "dict_cols_to_json",
                        return_value=empty_data,
                    ):
                        result = preprocess.execute(empty_data, "openaq-api")
                        assert result.empty


def test_from_options():
    filters = ["filter_pollutant", "filter_non_null_values"]
    preprocess = Preprocess.from_options(filters)
    assert preprocess.filter_pollutant
    assert preprocess.filter_non_null_values
    assert not preprocess.filter_extreme_values
    assert not preprocess.filter_no_coordinates


def test_from_options_all():
    filters = [
        "filter_pollutant",
        "filter_non_null_values",
        "filter_extreme_values",
        "filter_no_coordinates",
        "filter_countries",
        "filter_cities",
    ]
    preprocess = Preprocess.from_options(filters)
    assert preprocess.filter_pollutant
    assert preprocess.filter_non_null_values
    assert preprocess.filter_extreme_values
    assert preprocess.filter_no_coordinates
    assert preprocess.filter_countries
    assert preprocess.filter_cities

import os
from unittest.mock import patch

import pandas as pd
import pytest

from openaq_engine.src.preprocessing.filter import Filter


@pytest.fixture(autouse=True)
def mock_env_vars():
    with patch.dict(
        os.environ,
        {
            "DB_NAME_OPENAQ": "test_db",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
        },
    ):
        yield


@pytest.fixture
def sample_data():
    data = {
        "parameter": ["pm25", "pm10", "no2"],
        "coordinates": [
            "{latitude=37.7749, longitude=-122.4194}",
            "{}",
            "{latitude=34.0522, longitude=-118.2437}",
        ],
        "value": [10, -1, 600],
        "country": ["US", "CA", "MX"],
        "city": ["San Francisco", "Toronto", "Mexico City"],
    }
    return pd.DataFrame(data)


def test_filter_pollutant(sample_data):
    result = Filter.filter_pollutant(sample_data, "pm25")
    assert not result.empty
    assert result["parameter"].iloc[0] == "pm25"


def test_filter_no_coordinates(sample_data):
    result = Filter.filter_no_coordinates(sample_data)
    assert not result.empty
    assert "{}" not in result["coordinates"].values


def test_filter_non_null_values(sample_data):
    result = Filter.filter_non_null_values(sample_data)
    assert not result.empty
    assert all(result["value"] >= 0)


def test_filter_extreme_values(sample_data):
    result = Filter.filter_extreme_values(sample_data)
    assert not result.empty
    assert all(result["value"] <= 500)


def test_filter_countries():
    sample_data = pd.DataFrame(
        {
            "parameter": ["pm25", "no2"],
            "coordinates": [
                "{latitude=37.7749, longitude=-122.4194}",
                "{latitude=34.0522, longitude=-118.2437}",
            ],
            "value": [5, 600],
            "country": ["[US, CA]", "[MX, CA]"],
            "city": ["Toronto", "Mexico City"],
        }
    )

    result = Filter.filter_countries(sample_data, ["US", "MX"])
    assert not result.empty
    assert all(
        result["country"].apply(
            lambda x: any(country in x for country in ["US", "MX"])
        )
    )


def test_filter_cities():
    sample_data = pd.DataFrame(
        {
            "parameter": ["pm25", "no2"],
            "coordinates": [
                "{latitude=37.7749, longitude=-122.4194}",
                "{latitude=34.0522, longitude=-118.2437}",
            ],
            "value": [5, 600],
            "country": ["CA", "MX"],
            "city": ["[San Francisco, Toronto]", "[Mexico City]"],
        }
    )

    result = Filter.filter_cities(
        sample_data, ["San Francisco", "Mexico City"]
    )
    assert not result.empty
    assert all(
        result["city"].apply(
            lambda x: any(
                city in x for city in ["San Francisco", "Mexico City"]
            )
        )
    )

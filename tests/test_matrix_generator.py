import datetime
import os
import tempfile
from inspect import isclass
from unittest.mock import patch

import joblib
import pytest

from src.features.build_features import BuildFeaturesRandomForest
from src.matrix_generator import MatrixGenerator


def test_get_feature_generator():
    matrix_generator = MatrixGenerator(algorithm="RFR", id_column_list=[])
    feature_generator = matrix_generator._get_feature_generator()
    assert isclass(feature_generator)
    assert issubclass(feature_generator, BuildFeaturesRandomForest)


def test_get_feature_generator_invalid():
    matrix_generator = MatrixGenerator(algorithm="invalid", id_column_list=[])
    with pytest.raises(ValueError):
        matrix_generator._get_feature_generator()


def test_get_csr(mocker):
    matrix_generator = MatrixGenerator(algorithm="RFR", id_column_list=[])
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

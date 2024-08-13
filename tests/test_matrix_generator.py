import datetime
import os
import tempfile
from contextlib import nullcontext
from inspect import isclass
from unittest.mock import patch

import joblib
import pandas as pd
import pytest
from setup_environment import get_dbengine

from openaq_engine.src.features.build_features import BuildFeaturesRandomForest
from openaq_engine.src.matrix_generator import MatrixGenerator


def test_execute_for_cohort(mocker):
    # Mock the required arguments
    with nullcontext():
        engine = get_dbengine()
        training_validation_id = 0
        cohorts_df = pd.DataFrame({"train_validation_set": [0]})
        run_date = datetime.date(2020, 1, 1)

        # Define mock DataFrames to return
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col2": [3, 4]})
        feature_train_id = [1, 2]
        feature_valid_id = [3, 4]
        df3 = pd.DataFrame({"col3": [5, 6]})
        df4 = pd.DataFrame({"col3": [7, 8]})

        mocker.patch.object(MatrixGenerator, "_write_labels_as_csv")
        mocker.patch.object(
            MatrixGenerator,
            "matrix_generator",
            return_value=(
                df1,
                df2,
                feature_train_id,
                feature_valid_id,
                df3,
                df4,
            ),
            autospec=True,
            kwargs=True,
        )
        matrix_generator = MatrixGenerator(algorithm="RFR", id_column_list=[])

        # Call the method
        matrix_generator.execute_for_cohort(
            engine, training_validation_id, cohorts_df, run_date
        )

        with pytest.raises(ValueError):
            matrix_generator.matrix_generator.assert_called_with(
                matrix_generator, engine, cohorts_df
            )

        matrix_generator.matrix_generator.assert_called()

        matrix_generator._write_labels_as_csv.assert_any_call(
            df3, run_date, training_validation_id, "training"
        )


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
                0, "training", datetime.datetime(2020, 1, 1)
            )

    assert result == mock_data

    # Clean up the temporary file
    os.remove(tmp_file_path)

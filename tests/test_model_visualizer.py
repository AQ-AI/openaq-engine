import unittest
from unittest.mock import MagicMock, patch
import os

import pandas as pd
import pytest
from sqlalchemy import create_engine

from openaq_engine.src.model_visualizer import ModelVisualizer


@pytest.fixture
def setup_environment():
    # Setup any necessary environment variables or configurations here
    os.environ["TEST_PGDATABASE"] = "test_db"
    os.environ["TEST_PGUSER"] = "test_user"
    os.environ["TEST_PGPASSWORD"] = "test_password"
    os.environ["TEST_PGHOST"] = "localhost"
    os.environ["PGPORT"] = "5432"
    yield
    # Teardown logic if needed


class TestModelVisualizer(unittest.TestCase):
    def setUp(self):
        plot = True
        plot_metrics = ["R2", "MSE", "MAPE"]
        plots_table_name = "test_plots"
        results_table_name = "test_results"
        all_model_features = ["feature1", "feature2"]
        self.visualizer = ModelVisualizer(
            plot,
            plot_metrics,
            plots_table_name,
            results_table_name,
            all_model_features,
        )

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.figure.Figure.savefig")
    def test_execute(self, mock_savefig, mock_subplots):
        mock_subplots.return_value = (MagicMock(), MagicMock())

        validation_df = pd.DataFrame(
            {
                "feature1": [1, 2, 3],
                "feature2": [4, 5, 6],
            }
        )
        valid_pred = [1.1, 2.1, 3.1]
        valid_labels = [1, 2, 3]
        run_date = "2024-01-01 00:00:00"
        model_name = "test_model"
        results_metrics_df = pd.DataFrame(
            {
                "model_id": ["model_1", "model_2", "model_3"],
                "R2": [0.9, 0.8, 0.85],
                "MSE": [0.1, 0.2, 0.15],
                "MAPE": [0.05, 0.04, 0.03],
            }
        )

        self.visualizer.execute(
            validation_df,
            valid_pred,
            valid_labels,
            run_date,
            model_name,
            results_metrics_df,
        )

    @patch("openaq_engine.src.utils.utils.get_data")
    @patch("openaq_engine.setup_environment.get_dbengine")
    @pytest.mark.usefixtures("setup_environment")
    def test_get_results(self, mock_get_dbengine, mock_get_data):
        # Mock the return value of get_data
        mock_get_data.return_value = pd.DataFrame(
            {
                "model_id": ["model_1", "model_1", "model_1"],
                "run_date": [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:00",
                ],
                "metric_name": ["R2", "MSE", "MAPE"],
                "metric_value": [0.9, 0.1, 0.05],
            }
        )

        # Mock the return value of get_dbengine
        mock_get_dbengine.return_value = create_engine(
            "postgresql://test_user:test_password@localhost:5432/test_db"
        )

        # Call the method under test
        results = self.visualizer.get_results(
            run_date="2024-01-01 00:00:00", use_test_db=True
        )

        # Assertions to check if the results are as expected
        assert len(results) == 3
        assert results["model_id"].iloc[0] == "model_1"


if __name__ == "__main__":
    unittest.main()

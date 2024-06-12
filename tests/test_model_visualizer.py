import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from openaq_engine.src.model_visualizer import ModelVisualizer


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
    def test_get_results(self, mock_get_data):
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

        results = self.visualizer.get_results(
            run_date="2024-01-01 00:00:00", use_test_db=True
        )
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3)
        self.assertEqual(
            results["metric_name"].tolist(), ["R2", "MSE", "MAPE"]
        )


if __name__ == "__main__":
    unittest.main()

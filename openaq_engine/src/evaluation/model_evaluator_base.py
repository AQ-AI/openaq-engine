from abc import ABC

import mlflow
from sqlalchemy import text
from src.utils.utils import write_to_db


class ModelEvaluatorBase(ABC):
    def __init__(self, id_var):
        self.id_var = id_var

    def _results_to_db(
        self,
        results,
        table_name,
        engine,
    ):
        """Write model results to the database for all metrics and constraints"""
        # Define columns for the database
        columns_to_add = [
            x + " numeric"
            for x in [
                "mse",
                "mape",
            ]  # Metrics that will be logged to both DB and MLflow
        ]

        # Create table if not exists
        with engine.begin() as connection:
            connection.execute(
                text(
                    """CREATE TABLE IF NOT EXISTS {table} (
                        model_id text,
                        run_date timestamp,
                        cohort integer,
                        {extra_cols})
                    """.format(
                        table=table_name,
                        extra_cols=",".join(columns_to_add),
                    )
                )
            )

        # Write results to the database
        write_to_db(
            results,
            engine,
            table_name,
            "public",
            "append",
        )

    def _log_metrics_to_mlflow(self, run_id, metric, metric_value):
        """
        Log metrics to MLflow using the specified run_id.
        """
        mlflow.log_metric(metric, float(metric_value))

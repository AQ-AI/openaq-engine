import logging

import mlflow
import pandas as pd
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
from src.evaluation.model_evaluator_base import ModelEvaluatorBase

from config.model_settings import ModelEvaluatorConfig


class ModelEvaluator(ModelEvaluatorBase):
    def __init__(
        self,
        metrics,
        summary,
        valid_models,
    ) -> None:
        super().__init__(
            id_var=None
        )  # Initialize parent class with id_var if applicable
        self.metrics = metrics
        self.summary = summary
        self.valid_models = valid_models

    @classmethod
    def from_dataclass_config(
        cls, config: ModelEvaluatorConfig
    ) -> "ModelEvaluator":
        """Imports data from the config class"""
        return cls(
            metrics=config.METRICS,
            summary=config.SUMMARY_METHOD,
            valid_models=config.VALID_MODELS,
        )

    def execute(
        self,
        i,
        train_model,
        model_name,
        model_id,
        validation_df,
        valid_labels,
        start_datetime,
        engine,
        run_id,
    ):
        """
        Evaluate performance of trained model based on precision, recall, or accuracy.
        Writes the results table to the database.

        Parameters
        ----------
        i : int
            Cohort index.
        train_model : object
            Trained model to be evaluated.
        model_name : str
            Name of the model.
        model_id : str
            Identifier for the model run.
        validation_df : DataFrame
            DataFrame containing validation features.
        valid_labels : array-like
            Ground truth labels for validation data.
        start_datetime : datetime
            Start time of the evaluation.
        engine : sqlalchemy.engine
            Database engine for writing results.
        run_id : str
            MLflow run_id for logging metrics.
        """
        # Validate the model name against the list of valid models
        if model_name not in self.valid_models:
            logging.warning(
                f"Classifier {model_name} is not valid. Check valid models list."
            )
            return None

        logging.info("Evaluating all models")
        # Evaluate predictions and metrics
        valid_pred = train_model.predict(validation_df)
        metric_value = pd.DataFrame(
            {"model_id": model_id, "cohort": i}, index=[0]
        )

        # Ensure an active MLflow run and log metrics using run_id
        active_run = mlflow.active_run()
        if active_run:
            run_id = active_run.info.run_id

        eval_list = []
        for metric in self.metrics:
            eval = self.evaluate_one_metric(
                metric_value,
                metric,
                valid_labels,
                valid_pred,
                run_id,
            )
            eval_list.append(eval)

        results_metrics_df = pd.concat(eval_list)

        # Log metrics and write results to the database

        # Write the results to the database
        self._results_to_db(
            results_metrics_df,
            "results",
            engine,
        )
        return valid_pred, results_metrics_df

    def evaluate_one_metric(
        self,
        metric_value,
        metric,
        valid_labels,
        valid_pred,
        run_id,
    ):
        """Calculate evaluation metrics for one metric and constraint."""
        calc = None  # Initialize as None for safety checks

        if metric == "mse":
            calc = mean_squared_error(valid_labels, valid_pred)
            logging.info(f"{metric}: {calc}")

        elif metric == "mape":
            calc = mean_absolute_percentage_error(valid_labels, valid_pred)
            logging.info(f"{metric}: {calc}")

        # Add the calculated metric to the metric_value DataFrame
        if calc is not None:
            metric_value[metric] = calc

        self._log_metrics_to_mlflow(run_id, metric, metric_value[metric])

        return metric_value

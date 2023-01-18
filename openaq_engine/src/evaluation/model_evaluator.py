import logging

import pandas as pd
from sklearn.metrics import (
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from src.evaluation.model_evaluator_base import ModelEvaluatorBase

from config.model_settings import ModelEvaluatorConfig


class ModelEvaluator(ModelEvaluatorBase):
    def __init__(
        self,
        metrics,
        summary,
        valid_models,
    ) -> None:
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
        train_model,
        model_name,
        model_id,
        validation_df,
        valid_labels,
        start_datetime,
        engine,
    ):
        """
        Evaluate performance of trained model based on precision, recall, or accuracy.
        Writes the results table to the database

        Parameters
        ----------
        model_name : str
        model_id : str
        mlb_categories : list
        valid_y : dataframe
        """

        if model_name not in self.valid_models:
            logging.warning(
                f"Classifier {model_name} is not valid. Check valid models"
                " list."
            )
            return None

        logging.info("Evaluating all models")
        # iterate through all numeric constraints and metrics
        eval_list = []
        valid_pred = train_model.predict(validation_df)
        validation_df = validation_df.assign(
            actual=valid_labels, predicted=valid_pred
        )
        print(validation_df)
        metric_value = pd.DataFrame()

        for metric in self.metrics:
            eval = self.evaluate_one_metric(
                metric,
                valid_labels,
                valid_pred,
                start_datetime,
                metric_value,
                model_id,
            )
            eval_list += [eval]
        results_metrics_df = pd.concat(eval_list)

        # write the results to the db
        self._results_to_db(
            results_metrics_df,
            "results",
            start_datetime,
            engine,
        )

    def evaluate_one_metric(
        self,
        metric,
        valid_labels,
        valid_pred,
        start_datetime,
        metric_value,
        model_id,
    ):
        """Calculate evaluation metrics for one metric and constraint"""
        # select only the ranked values within the constraint

        # calculate the metric (e.g., precision, recall, or accuracy)
        # for each row in the data

        metric_value["model_id"] = model_id
        if metric == "R2":
            logging.info(f"{metric}: {r2_score(valid_labels, valid_pred)}")
            calc = r2_score(valid_labels, valid_pred)
        if metric == "MSE":
            logging.info(
                f"{metric}: {mean_squared_error(valid_labels, valid_pred)}"
            )
            calc = mean_squared_error(valid_labels, valid_pred)

        if metric == "MAPE":
            logging.info(
                f"{metric}:"
                f" {mean_absolute_percentage_error(valid_labels, valid_pred)}"
            )
            calc = mean_absolute_percentage_error(valid_labels, valid_pred)

        metric_value[f"{metric}"] = calc

        return metric_value

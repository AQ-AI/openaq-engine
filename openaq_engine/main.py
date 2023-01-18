import logging
import os
from datetime import datetime

import click
import mlflow
from mlflows.cli.cohort_builder import cohort_builder_options
from mlflows.cli.features.build_features import feature_builder_options
from mlflows.cli.time_splitter import time_splitter_options
from setup_environment import get_dbengine
from src.cohort_builder import CohortBuilder
from src.features.build_features import BuildFeaturesRandomForest
from src.matrix_generator import MatrixGenerator
from src.time_splitter import TimeSplitter
from src.train_model import ModelTrainer

from config.model_settings import (
    BuildFeaturesConfig,
    CohortBuilderConfig,
    MatrixGeneratorConfig,
    ModelTrainerConfig,
    TimeSplitterConfig,
)

mlflow.set_tracking_uri(
    os.getenv("MLFLOW_TRACKING_URI"),
)


class TimeSplitterFlow:
    def __init__(self) -> None:
        self.config = TimeSplitterConfig()

    def execute(self):
        return TimeSplitter.from_dataclass_config(
            self.config,
        )


class CohortBuilderFlow:
    def __init__(self):
        self.config = CohortBuilderConfig()

    def execute(self):
        return CohortBuilder.from_dataclass_config(
            self.config,
        )


class BuildFeaturesFlow:
    def __init__(self):
        self.config = BuildFeaturesConfig()

    def execute(self):
        # Trigger the authentication flow.
        return BuildFeaturesRandomForest.from_dataclass_config(
            self.config,
        )


class MatrixGeneratorFlow:
    def __init__(self):
        self.config = MatrixGeneratorConfig()

    def execute(self):
        # Trigger the authentication flow.
        return MatrixGenerator.from_dataclass_config(
            self.config,
        )


class ModelTrainerFlow:
    def __init__(self):
        self.config = ModelTrainerConfig()

    def execute(self):
        return ModelTrainer.from_dataclass_config(self.config)


@time_splitter_options()
@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter(city, country, sensor_type, source, pollutant, latest_date):
    experiment_id = mlflow.create_experiment(
        f"time_splitter_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id, nested=True):
        time_splitter = TimeSplitterFlow().execute()
        time_splitter.execute(
            city, country, sensor_type, source, pollutant, latest_date
        )


@cohort_builder_options()
@click.command("cohort-builder", help="Generate cohorts for time splits")
def cohort_builder(country, source, pollutant, latest_date):
    experiment_id = mlflow.create_experiment(
        f"cohort_builder_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id, nested=True):
        # initialize engine
        engine = get_dbengine()
        time_splitter = TimeSplitterFlow().execute()
        train_validation_dict = time_splitter.execute(
            country, source, pollutant, latest_date
        )

        cohort_builder = CohortBuilderFlow().execute()
        cohort_builder.execute(
            train_validation_dict, engine, country, source, pollutant
        )


@feature_builder_options()
@click.command("feature-builder", help="Generate features for cohorts")
def feature_builder(country, pollutant):
    experiment_id = mlflow.create_experiment(
        f"feature_builder_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id, nested=True):
        engine = get_dbengine()

        build_features = BuildFeaturesFlow().execute()
        build_features.execute(engine, country, pollutant)


@time_splitter_options()
@click.argument("models_directory")
@click.command("run-pipeline", help="Run all pipeline")
def run_pipeline(
    city,
    country,
    sensor_type,
    source,
    pollutant,
    latest_date,
    models_directory,
):
    start_datetime = datetime.now()
    logging.info(f"Starting pipeline at {start_datetime}")

    experiment_id = mlflow.create_experiment(
        f"run_pipeline_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id):
        # initialize engine
        engine = get_dbengine()
        time_splitter = TimeSplitterFlow().execute()
        train_validation_dict = time_splitter.execute(
            city, country, sensor_type, source, pollutant, latest_date
        )

        cohort_builder = CohortBuilderFlow().execute()
        cohort_builder.execute(
            train_validation_dict, engine, country, source, pollutant
        )
        matrix_generator = MatrixGeneratorFlow().execute()

        train_validation_set = matrix_generator.execute_train_valid_set()

        # loop for time splits
        model_output = []
        for i in train_validation_set:

            start_model_datetime = datetime.now()

            (
                validation_df,
                full_features_df,
                valid_labels,
                train_labels,
            ) = matrix_generator.execute(engine, i, start_datetime)
            logging.info(
                f"Starting pipeline for model {i} {start_model_datetime}"
            )
            model_trainer = ModelTrainerFlow().execute()
            model_output += model_trainer.train_all_models(
                i,
                full_features_df,
                train_labels,
                models_directory,
                start_datetime,
                engine,
            )


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(time_splitter)
cli.add_command(cohort_builder)
cli.add_command(feature_builder)
cli.add_command(run_pipeline)


if __name__ == "__main__":
    cli()

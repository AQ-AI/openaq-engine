import logging
import os
from datetime import datetime

import click
import mlflow
from mlflows.cli.cohort_builder import cohort_builder_options
from mlflows.cli.features.build_features import feature_builder_options
from mlflows.cli.time_splitter import time_splitter_options
from mlflows.cli.train_model import train_model_options
from setup_environment import get_dbengine
from sqlalchemy.sql import text
from src.cohort_builder import CohortBuilder
from src.evaluation.model_evaluator import ModelEvaluator
from src.features.build_features import BuildFeaturesRandomForest
from src.matrix_generator import MatrixGenerator
from src.model_visualizer import ModelVisualizer
from src.time_splitter import TimeSplitter
from src.train_model import ModelTrainer
from src.utils.utils import get_data, load_data_for_single_tv_set

from config.model_settings import (
    BuildFeaturesConfig,
    CohortBuilderConfig,
    MatrixGeneratorConfig,
    ModelEvaluatorConfig,
    ModelTrainerConfig,
    ModelVisualizerConfig,
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


class ModelEvaluatorFlow:
    def __init__(self):
        self.config = ModelEvaluatorConfig()

    def execute(self):
        return ModelEvaluator.from_dataclass_config(self.config)


class ModelVisualizerFlow:
    def __init__(self, plots_directory):
        self.config = ModelVisualizerConfig()
        self.eval_config = ModelEvaluatorConfig()
        self.plots_directory = plots_directory

    def execute(
        self,
        validation_df,
        valid_pred,
        valid_labels,
        start_datetime,
        model_name,
        results_metrics_df,
    ):
        return ModelVisualizer.from_dataclass_config(
            self.config, self.eval_config
        )


@time_splitter_options()
@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter(country, source, pollutant, latest_date):
    experiment_id = mlflow.create_experiment(
        f"time_splitter_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id, nested=True):
        time_splitter = TimeSplitterFlow().execute()
        time_splitter.execute(country, source, pollutant, latest_date)


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


@click.command("feature-builder", help="Generate features for cohorts")
@feature_builder_options()
@click.argument("models_directory")
@click.argument("plots_directory")
def feature_builder(models_directory, plots_directory, cohort_table):
    start_datetime = datetime.now()
    logging.info(f"Starting pipeline at {start_datetime}")

    experiment_id = mlflow.create_experiment(
        f"feature_builder_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )

    with mlflow.start_run(experiment_id=experiment_id, nested=True):
        engine = get_dbengine(
            os.getenv("PGDATABASE"),
            os.getenv("PGHOST"),
            os.getenv("PGPORT"),
            os.getenv("PGUSER"),
            os.getenv("PGPASSWORD"),
        )

        matrix_generator = MatrixGeneratorFlow().execute()

        locations_query = (
            f"""SELECT DISTINCT "x", "y" FROM "{cohort_table}";"""
        )
        # non_processed_locations_query = f"""SELECT c.x, c.y
        #     FROM (
        #         SELECT DISTINCT x, y FROM "{cohort_table}"
        #     ) c
        #     LEFT JOIN (
        #         SELECT DISTINCT longitude, latitude FROM "MODIS_061_MCD19A2_GRANULES_local_MN_new"
        #     ) s
        #     ON c.x = s.longitude AND c.y = s.latitude
        #     WHERE s.longitude IS NULL AND s.latitude IS NULL;"""

        locations_df = get_data(locations_query)

        for _, row in locations_df.iterrows():
            x = row["x"]
            y = row["y"]

            matrix_generator.execute(engine, x, y, cohort_table)
        matrix_generator.build_features(cohort_table)

    end_datetime = datetime.now()
    logging.info(f"Ending pipeline at {end_datetime}")
    logging.info(f"Total time elapsed: {end_datetime - start_datetime}")


@train_model_options()
@click.argument("models_directory")
@click.argument("plots_directory")
@click.command("run-pipeline", help="Run all pipeline")
def run_pipeline(
    models_directory,
    plots_directory,
    cohort_table,
    features,
    city,
    country,
):
    start_datetime = datetime.now()
    logging.info(f"Starting pipeline at {start_datetime}")

    experiment_id = mlflow.create_experiment(
        f"run_pipeline_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
    )
    engine = get_dbengine(
        os.getenv("PGDATABASE"),
        os.getenv("PGHOST"),
        os.getenv("PGPORT"),
        os.getenv("PGUSER"),
        os.getenv("PGPASSWORD"),
    )

    # Start the MLflow run and capture the run_id
    with mlflow.start_run(experiment_id=experiment_id, nested=True) as run:
        run_id = run.info.run_id  # Capture the run_id for logging

        matrix_generator = MatrixGeneratorFlow().execute()

        if features:
            logging.info(
                "Satellites data generated. Starting feature building"
            )
        else:
            locations_query = (
                f"""SELECT DISTINCT "x", "y" FROM "{cohort_table}";"""
            )

            locations_df = get_data(locations_query)

            for _, row in locations_df.iterrows():
                x = row["x"]
                y = row["y"]

                matrix_generator.execute(engine, x, y, cohort_table)

            matrix_generator.build_features(cohort_table)

        query = text(
            f"""WITH tv_sets AS (
                SELECT UNNEST(
                    string_to_array(
                        trim(both '{{}}' from tv_set),
                        ','
                    )::int[]
                ) AS tv_set
                FROM "{cohort_table}_training"

                UNION ALL

                SELECT UNNEST(
                    string_to_array(
                        trim(both '{{}}' from tv_set),
                        ','
                    )::int[]
                ) AS tv_set
                FROM "{cohort_table}_validation"
            )

            SELECT DISTINCT tv_set
            FROM tv_sets;"""
        )

        tv_sets_df = get_data(query)
        train_validation_set = tv_sets_df["tv_set"].tolist()

        model_output = []

        for i in train_validation_set:
            if i in [3, 4]:
                start_model_datetime = datetime.now()

                logging.info(
                    f"Starting pipeline for model {i} {start_model_datetime}"
                )
                X_train, Y_train, X_valid, Y_valid = (
                    load_data_for_single_tv_set(cohort_table, i)
                )

                model_trainer = ModelTrainerFlow().execute()
                model_output += model_trainer.train_all_models(
                    i,
                    X_train,
                    Y_train,
                    models_directory,
                    start_datetime,
                    engine,
                )
                logging.info("Getting model output")
                for model_id, model_name, train_model in model_output:
                    logging.info(f"Training and evaluating model {model_name}")
                    model_evaluator = ModelEvaluatorFlow().execute()
                    # Pass the run_id to the execute method of ModelEvaluator
                    valid_pred, results_metrics_df = model_evaluator.execute(
                        i,
                        train_model,
                        model_name,
                        model_id,
                        X_valid,
                        Y_valid,
                        start_datetime,
                        engine,
                        run_id=run_id,  # Pass run_id here
                    )
                    ModelVisualizerFlow(plots_directory).execute(
                        X_valid,
                        valid_pred,
                        Y_valid,
                        start_datetime,
                        model_name,
                        results_metrics_df,
                    )

                end_datetime = datetime.now()
                logging.info(f"Ending pipeline at {end_datetime}")
                logging.info(
                    f"Total time ellapsed: {end_datetime - start_datetime}"
                )


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    pass


cli.add_command(time_splitter)
cli.add_command(cohort_builder)
cli.add_command(feature_builder)
cli.add_command(run_pipeline)


if __name__ == "__main__":
    cli()

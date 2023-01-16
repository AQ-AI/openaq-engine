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
from src.time_splitter import TimeSplitter

from config.model_settings import (
    BuildFeaturesConfig,
    CohortBuilderConfig,
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


@time_splitter_options()
@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter(country, source, pollutant, latest_date):
    experiment_id = mlflow.create_experiment(
        f"time_splitter_{str(datetime.now())}"
    )

    with mlflow.start_run(experiment_id=experiment_id):
        time_splitter = TimeSplitterFlow().execute()
        time_splitter.execute(country, source, pollutant, latest_date)


@cohort_builder_options()
@click.command("cohort-builder", help="Generate cohorts for time splits")
def cohort_builder(country, source, pollutant, latest_date):
    experiment_id = mlflow.create_experiment(
        f"cohort_builder_{str(datetime.now())}"
    )

    with mlflow.start_run(experiment_id=experiment_id):
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
        f"feature_builder_{str(datetime.now())}"
    )

    with mlflow.start_run(experiment_id=experiment_id):
        engine = get_dbengine()

        build_features = BuildFeaturesFlow().execute()
        build_features.execute(engine, country, pollutant)


@time_splitter_options()
@click.command("run-pipeline", help="Run all pipeline")
def run_pipeline(country, source, pollutant, latest_date):
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
    build_features = BuildFeaturesFlow().execute()
    build_features.execute(engine, country, pollutant)


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

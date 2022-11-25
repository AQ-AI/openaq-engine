import click
from setup_environment import get_dbengine
from src.cohort_builder import CohortBuilder
from src.features.build_features import BuildFeaturesRandomForest
from src.time_splitter import TimeSplitter

from config.model_settings import (
    BuildFeaturesConfig,
    CohortBuilderConfig,
    TimeSplitterConfig,
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
        return BuildFeaturesRandomForest.from_dataclass_config(self.config)


@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter():
    time_splitter = TimeSplitterFlow().execute()
    time_splitter.execute()


@click.command("cohort-builder", help="Generate cohorts for time splits")
def cohort_builder():
    # initialize engine
    engine = get_dbengine()
    time_splitter = TimeSplitterFlow().execute()
    train_validation_dict = time_splitter.execute()

    cohort_builder = CohortBuilderFlow().execute()
    cohort_builder.execute(train_validation_dict, engine)


@click.command("feature-builder", help="Generate features for cohorts")
def feature_builder():
    engine = get_dbengine()

    build_features = BuildFeaturesFlow().execute()
    build_features.execute(engine)


@click.command("run-pipeline", help="Run all pipeline")
def run_pipeline():
    # initialize engine
    engine = get_dbengine()
    time_splitter = TimeSplitterFlow().execute()
    train_validation_dict = time_splitter.execute()

    cohort_builder = CohortBuilderFlow().execute()
    df = cohort_builder.execute(train_validation_dict, engine)
    build_features = BuildFeaturesFlow().execute()
    build_features.execute(df, save_images=False)


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(time_splitter)
cli.add_command(cohort_builder)
cli.add_command(feature_builder)


if __name__ == "__main__":
    cli()

import logging
import os
import tempfile

import click
import click_config_file
import mlflow
from mlflows.base_flow import BaseFlow
from mlflows.cli.mlflow_cli import experiment_options
from mlflows.cli.time_splitter_cli import time_splitter_options
from mlflows.utils import write_dataclass, yaml_provider

from openaq_engine.config.model_settings import TimeSplitterConfig
from openaq_engine.src.time_splitter import TimeSplitter

EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "time-splitter")


class TimeSplitterFlow(BaseFlow):
    def __init__(self, country, source, pollutant, latest_date) -> None:
        country = country
        source = source
        pollutant = pollutant
        latest_date = latest_date

    def execute_in_run(self, **kwargs):
        config = TimeSplitterConfig(
            COUNTRY=self.country,
            SOURCE=self.source,
            TARGET_VARIABLE=self.pollutant,
            LATEST_DATE=self.latest_date,
        )
        time_splitter = TimeSplitter.from_dataclass_config(
            config,
        )
        train_validation_dict = time_splitter.execute()
        self.log_run_details(
            train_validation_dict, config, kwargs.get("output_file")
        )
        if train_validation_dict.empty:
            logging.info(
                "No data found for any of the countries provided :"
                f" {self.country}"
            )

        return train_validation_dict

    def execute(self, **experiment_options):
        mlflow.set_experiment(
            experiment_options.pop("experiment_name", EXPERIMENT_NAME)
        )
        with mlflow.start_run(
            run_name=experiment_options.pop("run_name"),
            nested=experiment_options.pop("nested", False),
        ):
            train_validation_dict = self.execute_in_run(**experiment_options)

        return train_validation_dict

    def log_run_details(
        self,
        config: TimeSplitterConfig,
    ):
        with tempfile.TemporaryDirectory("w+") as dir_name:
            mlflow.log_params(
                {
                    "COUNTRY": config.COUNTRY,
                    "LATEST_DATE": config.LATEST_DATE,
                    "POLLUTANT": config.TARGET_VARIABLE,
                }
            )

            full_config_path = os.path.join(dir_name, "config.json")
            write_dataclass(config, full_config_path)
            mlflow.log_artifact(full_config_path)


@click.group()
@experiment_options(
    default_experiment_name=EXPERIMENT_NAME,
)
@click.pass_context
def cli(context, experiment_name, run_name, nested):
    context.obj = {
        "experiment_name": experiment_name,
        "run_name": run_name,
        "nested": nested,
    }


@time_splitter_options()
@click_config_file.configuration_option(provider=yaml_provider, implicit=False)
@click.pass_context
@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter(context, country, source, pollutant, latest_date):
    time_splitter = TimeSplitterFlow(country, source, pollutant, latest_date)

    time_splitter.execute(**context.obj)


cli.add_command(time_splitter)

if __name__ == "__main__":
    cli()

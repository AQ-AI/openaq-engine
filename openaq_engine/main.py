import click
from config.model_settings import TimeSplitterConfig, CohortBuilderConfig
from src.time_splitter import TimeSplitter
from src.cohort_builder import CohortBuilder


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


@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter():
    time_splitter = TimeSplitterFlow().execute()
    time_splitter.execute()


@click.command("cohort-builder", help="Generate cohorts for time splits")
def cohort_builder():
    time_splitter = TimeSplitterFlow().execute()
    train_validation_list = time_splitter.execute()

    cohort_builder = CohortBuilderFlow().execute()
    cohort_builder.execute(train_validation_list)


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(time_splitter)
cli.add_command(cohort_builder)
if __name__ == "__main__":
    cli()

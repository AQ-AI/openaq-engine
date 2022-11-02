import click
from config.model_settings import (
    HistoricOpenAQConfig,
    TimeSplitterConfig,
    CohortBuilderConfig,
)
from src.historic_openaq import HistoricOpenAQ
from src.time_splitter import TimeSplitter
from src.cohort_builder import CohortBuilder
from setup_environment import get_dbengine


class HistoricOpenAQFlow:
    def __init__(self) -> None:
        self.config = HistoricOpenAQConfig()

    def execute(self):
        historic_openaq = HistoricOpenAQ.from_dataclass_config(self.config)

        location, data = historic_openaq.execute()


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


@click.command(
    "query-historic-openaq", help="querying historic pm2.5 values from OpenAQ"
)
def query_historic_openaq():
    HistoricOpenAQFlow().execute()


@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter():
    TimeSplitterFlow().execute()


@click.command("cohort-builder", help="Generate cohorts for time splits")
def cohort_builder():
    # initialize engine
    engine = get_dbengine()
    print(engine)
    time_splitter = TimeSplitterFlow().execute()
    train_validation_list = time_splitter.execute()

    cohort_builder = CohortBuilderFlow().execute()
    cohort_builder.execute(train_validation_list, engine)


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(query_historic_openaq)
cli.add_command(time_splitter)
cli.add_command(cohort_builder)
if __name__ == "__main__":
    cli()

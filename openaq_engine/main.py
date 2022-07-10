import click
from config.model_settings import HistoricOpenAQConfig, TimeSplitterConfig
from src.historic_openaq import HistoricOpenAQ
from src.time_splitter import TimeSplitter


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
        time_splitter = TimeSplitter.from_dataclass_config(
            self.config,
        )

        return time_splitter.execute()


@click.command(
    "query-historic-openaq", help="querying historic pm2.5 values from OpenAQ"
)
def query_historic_openaq():
    HistoricOpenAQFlow().execute()


@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter():

    csv_list = TimeSplitterFlow().execute()


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(query_historic_openaq)
cli.add_command(time_splitter)

if __name__ == "__main__":
    cli()

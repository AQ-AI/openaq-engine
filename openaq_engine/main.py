import click
from config.model_settings import TimeSplitterConfig
from src.time_splitter import TimeSplitter


class TimeSplitterFlow:
    def __init__(self) -> None:
        self.config = TimeSplitterConfig()

    def execute(self):
        time_splitter = TimeSplitter.from_dataclass_config(
            self.config,
        )

        return time_splitter.execute()

@click.command("time-splitter", help="Splits csvs for time splits")
def time_splitter():

    csv_list = TimeSplitterFlow().execute()


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(time_splitter)

if __name__ == "__main__":
    cli()

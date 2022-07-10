import click
from config.model_settings import HistoricOpenAQConfig
from src.historic_openaq import HistoricOpenAQ


class HistoricOpenAQFlow:
    def __init__(self) -> None:
        self.config = HistoricOpenAQConfig()

    def execute(self):
        historic_openaq = HistoricOpenAQ.from_dataclass_config(self.config)

        location, data = historic_openaq.execute()


@click.command(
    "query-historic-openaq", help="querying historic pm2.5 values from OpenAQ"
)
def query_historic_openaq():
    HistoricOpenAQFlow().execute()


@click.group("openaq-engine", help="Library to query openaq data")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(query_historic_openaq)

if __name__ == "__main__":
    cli()

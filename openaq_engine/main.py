import os

import boto3
import click
from config.model_settings import HistoricOpenAQConfig
from src.historic_openaq import HistoricOpenAQ


class HistoricOpenAQFlow:
    def __init__(self) -> None:
        self.config = HistoricOpenAQConfig()

    def execute(self):
        historic_openaq = HistoricOpenAQ.from_dataclass_config(self.config)

        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("ACCESS_ID"),
            aws_secret_access_key=os.getenv("ACCESS_KEY"),
        )
        session = boto3.Session()

        location, data = historic_openaq.execute(session)


@click.command(
    "query-historic-openaq", help="querying historic pm1.5 values from OpenAQ"
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

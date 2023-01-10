import click
from click_option_group import OptionGroup
from mlflows.utils import parametrized

from config.model_settings import TimeSplitterConfig


@parametrized
def time_splitter_options(fn, countries_option: bool = True):
    """
    countries_option: bool = True
        Whether to provide the option to specify countries or not
    """
    time_splitter_config = OptionGroup(
        "Options for defining time-splitter",
        help=(
            "Allows definition of custom timesplits for provided "
            "countries and from the date provided"
        ),
    )
    country_ = time_splitter_config.option(
        "-c",
        "--country",
        default=TimeSplitterConfig.COUNTRY,
        type=click.STRING,
        help=(
            "Load timesplits from specific countries in the 'Country Code'"
            " format e.g. 'IN' (India)"
        ),
    )
    pollutant = time_splitter_config.option(
        "-p",
        "--pollutant",
        default=TimeSplitterConfig.TARGET_VARIABLE,
        type=click.Choice(
            [
                "co",
                "no2",
                "o3",
                "pm1",
                "pm10",
                "pm25",
                "so2",
            ]
        ),
        help="Load timesplits from data for the pollutant requested",
    )
    latest_date = time_splitter_config.option(
        "-d",
        "--latest-date",
        hidden=True,
        type=click.STRING,
        help="Date to load data until in format YYYY-MM-DD",
    )
    source = time_splitter_config.option(
        "-s",
        "--source",
        default=TimeSplitterConfig.SOURCE,
        type=click.Choice(["openaq-aws", "openaq-api"]),
        help="Source to load the openaq data from",
    )
    wrapped_func = source(pollutant(latest_date(fn)))
    if countries_option:
        wrapped_func = country_(wrapped_func)

    return wrapped_func

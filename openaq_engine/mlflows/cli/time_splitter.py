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
    countries_ = time_splitter_config.option(
        "-c",
        "--countries",
        default=TimeSplitterConfig.COUNTRY_BOUNDING_BOXES.get("WO"),
        type=click.STRING,
        help="Countries to load data from",
    )
    pollutant = time_splitter_config.option(
        "-p",
        "--pollutant",
        type=click.STRING,
        default=TimeSplitterConfig.TARGET_VARIABLE,
        help="Load data with the provided number of bedrooms",
    )
    latest_date = time_splitter_config.option(
        "-d",
        "--latest-date",
        default=TimeSplitterConfig.LATEST_DATE,
        type=click.STRING,
        help="Date to load data until in format YYYY-MM-DD",
    )
    wrapped_func = pollutant(latest_date(fn))
    if countries_option:
        wrapped_func = countries_(wrapped_func)

    return wrapped_func

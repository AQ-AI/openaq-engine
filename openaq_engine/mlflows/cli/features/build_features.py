import click
from click_option_group import OptionGroup
from mlflows.utils import parametrized

from config.model_settings import BuildFeaturesConfig


@parametrized
def feature_builder_options(fn, countries_option: bool = True):
    """
    countries_option: bool = True
        Whether to provide the option to specify countries or not
    """
    cohort_builder_config = OptionGroup(
        "Options for defining the cohort",
        help=(
            "Allows definition of custom cohorts for provided "
            "countries and from the date provided"
        ),
    )
    country_ = cohort_builder_config.option(
        "-c",
        "--country",
        default=BuildFeaturesConfig.COUNTRY,
        type=click.STRING,
        help=(
            "Load timesplits from specific countries in the 'Country Code'"
            " format e.g. 'IN' (India)"
        ),
    )
    pollutant = cohort_builder_config.option(
        "-p",
        "--pollutant",
        default=BuildFeaturesConfig.TARGET_VARIABLE,
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
    latest_date = cohort_builder_config.option(
        "-d",
        "--latest-date",
        hidden=True,
        type=click.STRING,
        help="Date to load data until in format YYYY-MM-DD",
    )
    wrapped_func = pollutant(latest_date(fn))
    if countries_option:
        wrapped_func = country_(wrapped_func)
    return wrapped_func

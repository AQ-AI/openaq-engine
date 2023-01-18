import click
from click_option_group import OptionGroup
from mlflows.utils import parametrized

from config.model_settings import CohortBuilderConfig


@parametrized
def cohort_builder_options(
    fn, countries_option: bool = True, cities_option: bool = True
):

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
        default=CohortBuilderConfig.COUNTRY,
        type=click.STRING,
        help=(
            "Load timesplits from specific countries in the 'Country Code'"
            " format e.g. 'IN' (India)"
        ),
    )
    city = cohort_builder_config.option(
        "-ci",
        "--city",
        default=CohortBuilderConfig.CITY,
        type=click.STRING,
        help="Load timesplits from a specific city",
    )
    sensor_type = cohort_builder_config.option(
        "-s",
        "--sensor-type",
        default=CohortBuilderConfig.SENSOR_TYPE,
        type=click.Choice(["reference grade", "low-cost sensor"]),
        help="Load timesplits from data for the sensor type requested",
    )
    pollutant = cohort_builder_config.option(
        "-p",
        "--pollutant",
        default=CohortBuilderConfig.TARGET_VARIABLE,
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
        help="Load cohorts from data for the pollutant requested",
    )
    latest_date = cohort_builder_config.option(
        "-d",
        "--latest-date",
        hidden=True,
        type=click.STRING,
        help="Date to load data until in format YYYY-MM-DD",
    )
    source = cohort_builder_config.option(
        "-s",
        "--source",
        default=CohortBuilderConfig.SOURCE,
        type=click.Choice(["openaq-aws", "openaq-api"]),
        help="Source to load the openaq data from",
    )
    wrapped_func = source(sensor_type(pollutant(latest_date(fn))))
    if countries_option:
        wrapped_func = country_(wrapped_func)
    if cities_option:
        wrapped_func = city(wrapped_func)

    return wrapped_func

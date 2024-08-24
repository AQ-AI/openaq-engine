import click
from click_option_group import OptionGroup
from mlflows.utils import parametrized


@parametrized
def train_model_options(fn):
    """
    Modify the CLI options to include cohort-table.
    """
    train_model_config = OptionGroup(
        "Options for defining the training model",
        help="Allows definition of the cohort table to be used, and whether features are generated",
    )

    cohort_table = train_model_config.option(
        "-t",
        "--cohort-table",
        type=click.STRING,
        help="Name of the table containing cohort data",
    )
    features = train_model_config.option(
        "-f",
        "--features",
        is_flag=True,
        default=False,
        help="Whether features have been generated",
    )
    city = train_model_config.option(
        "--city",
        type=click.STRING,
        default=None,
        help="Name of the city to filter the data (optional)",
    )
    country = train_model_config.option(
        "--country",
        type=click.STRING,
        default=None,
        help="Name of the country to filter the data (optional)",
    )
    return country(city(features(cohort_table(fn))))

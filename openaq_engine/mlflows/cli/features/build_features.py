import click
from click_option_group import OptionGroup
from mlflows.utils import parametrized

from config.model_settings import BuildFeaturesConfig


@parametrized
def feature_builder_options(fn):
    """
    Modify the CLI options to include cohort-table.
    """
    cohort_builder_config = OptionGroup(
        "Options for defining the cohort",
        help="Allows definition of custom cohorts using the provided cohort table",
    )

    cohort_table = cohort_builder_config.option(
        "-t",
        "--cohort-table",
        default=BuildFeaturesConfig.TABLE_NAME,
        type=click.STRING,
        help="Name of the table containing cohort data",
    )

    return cohort_table(fn)

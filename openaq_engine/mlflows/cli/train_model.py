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

    return features(cohort_table(fn))

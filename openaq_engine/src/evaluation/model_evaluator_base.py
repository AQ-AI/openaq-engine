from abc import ABC

from sqlalchemy import text
from src.utils.utils import write_to_db


class ModelEvaluatorBase(ABC):
    def __init__(self, id_var):
        self.id_var = id_var

    def _results_to_db(
        self,
        results,
        table_name,
        engine,
    ):
        """Write model results to the database for all metrics and constraints"""

        columns_to_add = [
            x + " numeric"
            # hardcoded as they should not change
            # even if self.metrics change, the columns should just
            # be blank to prevent issues when appending data later
            for x in ["mse", "mape"]
        ]

        with engine.begin() as connection:
            connection.execute(
                text(
                    """CREATE TABLE IF NOT EXISTS {table} (
                        model_id text,
                        run_date timestamp,
                        cohort integer,
                        {extra_cols})
                    """.format(
                        table=table_name,
                        extra_cols=",".join(columns_to_add),
                    )
                )
            )

        write_to_db(
            results,
            engine,
            table_name,
            "public",
            "append",
        )

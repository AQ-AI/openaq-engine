import os
import logging
from abc import ABC
from itertools import chain
from typing import Any, Dict
import pandas as pd
from joblib import Parallel, delayed

from config.model_settings import CohortBuilderConfig
from src.utils.utils import query_results, write_to_db


class CohortBuilderBase(ABC):
    def __init__(
        self,
        table_name: str,
        region_name: str,
        bucket: str,
        s3_output: str,
    ):
        self.table_name = table_name
        self.region_name = region_name
        self.bucket = bucket
        self.s3_output = s3_output

    def _build_response(self, params, sql_query):
        response_query_result = query_results(params, sql_query)
        header = [
            d["VarCharValue"]
            for d in response_query_result["ResultSet"]["Rows"][0]["Data"]
        ]
        rows = response_query_result["ResultSet"]["Rows"][1:]
        result = [
            dict(zip(header, self._get_var_char_values(row))) for row in rows
        ]
        return pd.DataFrame(result)

    def _get_var_char_values(self, row):
        return [d["VarCharValue"] for d in row["Data"] if "VarCharValue" in d]


class CohortBuilder(CohortBuilderBase):
    def __init__(
        self,
        date_col: str,
        filter_dict: Dict[str, Any],
    ) -> None:
        self.date_col = date_col
        self.filter_dict = filter_dict
        super().__init__(
            CohortBuilderConfig.TABLE_NAME,
            CohortBuilderConfig.REGION,
            CohortBuilderConfig.S3_BUCKET,
            CohortBuilderConfig.S3_OUTPUT,
        )

    @classmethod
    def from_dataclass_config(
        cls, config: CohortBuilderConfig
    ) -> "CohortBuilder":
        return cls(
            date_col=config.DATE_COL,
            filter_dict=config.FILTER_DICT,
        )

    def execute(self, train_validation_dict, engine):
        filter_cols = ", ".join(
            set(list(chain.from_iterable(self.filter_dict.values())))
        )

        cohorts_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.cohort_builder)(
                    cohort_type, train_validation_dict, filter_cols
                )
                for cohort_type in train_validation_dict.keys()
            ),
            axis=0,
        ).reset_index(drop=True)
        filtered_cohorts_df = (
            Preprocess()
            .from_options(list(self.filter_dict.keys()))
            .execute(cohorts_df)
        )

        self._results_to_db(engine, filtered_cohorts_df)

    def cohort_builder(
        self, cohort_type, train_validation_dict, filter_cols
    ) -> pd.DataFrame:
        """
        Retrieve coded er data data from train data.

        Ensures that dataframe always has columns mentioned in ENTITY_ID_COLUMNS
        even if dataframe is empty.

        Returns
        -------
        pd.DataFrame
            Cohort dataframe for openaq data
        """
        date_tup_list = list(train_validation_dict[f"{cohort_type}"])
        df_list = []
        params = {
            "region": str(self.region_name),
            "database": str(os.getenv("DB_NAME_OPENAQ")),
            "bucket": str(self.bucket),
            "path": f"{self.s3_output}/cohorts",
        }

        for index, date_tuple in enumerate(date_tup_list):
            query = """SELECT DISTINCT *
                FROM {table}
                WHERE {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}' LIMIT 100;""".format(
                table=self.table_name,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
            )

            df = self._build_response(params, query)
            df["train_validation_set"] = index
            df["cohort"] = f"{index}_{date_tuple[0]}_{date_tuple[1]}"
            df["cohort_type"] = f"{cohort_type}"
            if df.empty:
                logging.info(
                    f"""No openaq data found for
                     {date_tuple[0].date()}_{date_tuple[1].date()} time window"""
                )

            df_list.append(df)
        cohort_df = pd.concat(df_list, axis=0).reset_index(drop=True)
        return cohort_df

    def _results_to_db(self, filtered_cohorts_df, engine):
        """Write model results to the database for all cohorts"""

        write_to_db(
            engine,
            filtered_cohorts_df,
            "cohorts",
            "public",
            "replace",
        )

import logging
from abc import ABC
from itertools import chain
from typing import Dict, Any
import pandas as pd
from joblib import Parallel, delayed

from config.model_settings import CohortBuilderConfig
from src.utils.utils import query_results, get_data


class CohortBuilderBase(ABC):
    def __init__(
        self,
        database: str,
        region_name: str,
        bucket: str,
        s3_output: str,
    ):

        self.database = database
        self.region_name = region_name
        self.bucket = bucket
        self.s3_output = s3_output

    def _build_response(self, params, sql_query):
        response_query_result = query_results(params, sql_query)
        header = response_query_result["ResultSet"]["Rows"][0]
        rows = response_query_result["ResultSet"]["Rows"][1:]
        for row in rows:
            return self._get_var_char_values(row)

    def _get_var_char_values(self, d):
        for obj in d["Data"]:
            if obj["VarCharValue"]:
                return obj["VarCharValue"]
            else:
                pass


class CohortBuilder(CohortBuilderBase):
    def __init__(
        self,
        date_col: str,
        filter_dict: Dict[str, Any],
    ) -> None:
        self.date_col = date_col
        self.filter_dict = filter_dict
        super().__init__(
            CohortBuilderBase.TABLE_NAME,
            CohortBuilderBase.DATABASE,
            CohortBuilderBase.REGION,
            CohortBuilderBase.S3_BUCKET,
            CohortBuilderBase.S3_OUTPUT,
        )

    @classmethod
    def from_dataclass_config(cls, config: CohortBuilderConfig) -> "CohortBuilder":
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

        filtered_cohorts_df = Preprocess.from_options(
            list(self.filter_dict.keys())
        ).execute(cohorts_df)

        self._results_to_db(engine, filtered_cohorts_df, filter_cols)

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
            "database": str(self.database),
            "bucket": str(self.bucket),
            "path": f"{self.s3_output}/max_date",
        }

        for index, date_tuple in enumerate(date_tup_list):
            query = """SELECT DISTINCT ,{date_col},{filter_cols}
                FROM {table}
                WHERE {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}';""".format(
                table=self.table_name,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
                filter_cols=filter_cols,
            )

            df = self._build_response(params, query)
            df["train_validation_set"] = index
            df["cohort"] = f"{index}_{date_tuple[0].date()}_{date_tuple[1].date()}"
            df["cohort_type"] = f"{cohort_type}"
            if df.empty:
                logging.info(
                    f"""No openaq data found for
                     {date_tuple[0].date()}_{date_tuple[1].date()} time window"""
                )

            df_list.append(df)
        cohort_df = pd.concat(df_list, axis=0).reset_index(drop=True)
        return cohort_df

    def _results_to_db(self, engine, filtered_cohorts_df, filter_cols):
        """Write model results to the database for all cohorts"""
        filtered_cohorts_df_no_features = filtered_cohorts_df.drop(
            labels=list(filter_cols.split(", ")), axis=1
        )
        # write_to_db(
        #     filtered_cohorts_df_no_features,
        #     engine,
        #     "cohorts",
        #     self.schema_name,
        #     "replace",
        # )

    def filter_priority_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out rows which contain non-priority categories
        """
        priority_cats = self._list_priority_categories()

        return df.assign(
            priority_cats=(
                df.category.apply(
                    lambda cat: any(
                        str_.lower() in cat[1:-1].split(",") for str_ in priority_cats
                    )
                )
            )
        ).query("priority_cats == True")

    def _list_priority_categories(self):
        """Getting priority codes as a list"""
        priority_cat_query = """select icd_10_cm from {schema_name}.{table}
        where count >= {no_of_occurrences};""".format(
            schema_name=self.priority_schema_name,
            table=self.priority_table_name,
            no_of_occurrences=self.no_of_occurrences,
        )
        priority_cats = get_data(priority_cat_query)
        return list(priority_cats["icd_10_cm"])

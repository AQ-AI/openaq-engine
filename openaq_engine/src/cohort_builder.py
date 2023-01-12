import json
import logging
import os
from abc import ABC
from datetime import datetime
from itertools import chain
from typing import Any, Dict, List

import pandas as pd
from joblib import Parallel, delayed
from src.preprocess import Preprocess
from src.utils.utils import (
    query_results_from_api,
    query_results_from_aws,
    write_to_db,
)

from config.model_settings import CohortBuilderConfig


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

    def build_response_from_aws(self, params, sql_query):
        response_query_result = query_results_from_aws(params, sql_query)
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
        return [
            d["VarCharValue"] if "VarCharValue" in d else "{}"
            for d in row["Data"]
        ]


class CohortBuilder(CohortBuilderBase):
    def __init__(
        self,
        date_col: str,
        filter_dict: Dict[str, Any],
        target_variable: List[str],
        country: str,
        source: str,
    ) -> None:
        self.date_col = date_col
        self.filter_dict = filter_dict
        self.targete_variable = target_variable
        self.country = country
        self.source = source
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
            target_variable=config.TARGET_VARIABLE,
            country=config.COUNTRY,
            source=config.SOURCE,
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

        self._results_to_db(filtered_cohorts_df, engine)

    def cohort_builder(
        self,
        cohort_type,
        train_validation_dict,
        country,
        source,
        pollutant,
    ) -> pd.DataFrame:
        """
        Retrieve coded er data data from train data.

        Ensures that dataframe always has columns mentioned in
        ENTITY_ID_COLUMNS even if dataframe is empty.

        Returns
        -------
        pd.DataFrame
            Cohort dataframe for openaq data
        """
        date_tup_list = list(train_validation_dict[f"{cohort_type}"])
        df_list = []

        for index, date_tuple in enumerate(date_tup_list):
            if source == "openaq-aws":
                df = self.execute_for_openaq_aws(
                    date_tuple, country, pollutant
                )
            if source == "openaq-api":
                df = self.execute_for_openaq_api(
                    date_tuple, country, pollutant
                )

            df["train_validation_set"] = index
            df["cohort"] = f"{index}_{date_tuple[0]}_{date_tuple[1]}"
            df["cohort_type"] = f"{cohort_type}"
            if df.empty:
                logging.info(
                    f"""No openaq data found for
                    {date_tuple[0].date()}_{date_tuple[1].date()}
                    time window"""
                )

            df_list.append(df)
        cohort_df = pd.concat(df_list, axis=0).reset_index(drop=True)
        return cohort_df

    def execute_for_openaq_aws(self, date_tuple, country, pollutant):
        params = {
            "region": str(self.region_name),
            "database": str(os.getenv("DB_NAME_OPENAQ")),
            "bucket": str(os.getenv("S3_BUCKET_OPENAQ")),
            "path": f"{str(os.getenv('S3_OUTPUT_OPENAQ'))}/cohorts",
        }
        if pollutant:
            self.target_variable = pollutant
        if country == "WO":
            query = """SELECT DISTINCT *
                FROM {table}
                WHERE parameter='{target_variable}' AND {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}';""".format(
                table=self.table_name,
                target_variable=self.target_variable,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
            )

        else:
            query = """SELECT DISTINCT *
                FROM {table}
                WHERE parameter='{target_variable}' AND country='{country}'
                AND {date_col} BETWEEN '{start_date}' AND '{end_date}';""".format(
                table=self.table_name,
                target_variable=self.target_variable,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
                country=country,
            )
        return self.build_response_from_aws(params, query)

    def execute_for_openaq_api(
        self,
        date_tuple,
        country,
        pollutant,
    ):
        if pollutant:
            self.target_variable = pollutant
        if country == "WO":
            url = """https://api.openaq.org/v2/locations?limit=1000&page=1&
            offset=0&sort=asc&parameter={pollutant}&radius=1000&
            order_by=firstUpdated&dumpRaw=false""".format(
                pollutant=self.target_variable
            )
        else:
            url = """https://api.openaq.org/v2/measurements?
            date_from={date_from}
            date_to={date_to}
            limit=100&page=1&offset=0&sort=desc&radius=1000
            &order_by=datetime""".format(
                date_from=date_tuple[0], date_to=date_tuple[1]
            )
        headers = {"accept": "application/json"}

        response = query_results_from_api(headers, url)
        return datetime.strptime(
            json.loads(response)["results"][0]["firstUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        ).date()

    def _results_to_db(self, filtered_cohorts_df, engine):
        """Write model results to the database for all cohorts"""

        write_to_db(
            filtered_cohorts_df,
            engine,
            "cohorts",
            "public",
            "replace",
        )

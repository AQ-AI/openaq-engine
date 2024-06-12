import logging
import os
from abc import ABC
from datetime import datetime
from itertools import chain
from typing import Any, Dict, List

import mlflow
import pandas as pd
from joblib import Parallel, delayed
from setup_environment import get_dbengine
from src.preprocess import Preprocess
from src.utils.utils import (
    api_response_to_df,
    extract_utc_date,
    get_data,
    query_results_from_aws,
    write_to_db,
)

from config.model_settings import CohortBuilderConfig

logging.basicConfig(level=logging.INFO)


class CohortBuilderBase(ABC):
    def __init__(
        self, table_name: str, region_name: str, bucket: str, s3_output: str
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
        self.target_variable = target_variable
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

    def execute(
        self,
        train_validation_dict,
        city,
        country,
        source,
        sensor_type,
        pollutant,
        local_data,
    ):
        filter_cols = ", ".join(
            set(list(chain.from_iterable(self.filter_dict.values())))
        )

        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.cohort_builder)(
                cohort_type,
                train_validation_dict,
                filter_cols,
                city,
                country,
                source,
                sensor_type,
                pollutant,
                local_data,
            )
            for cohort_type in train_validation_dict.keys()
        )

        mlflow.log_param("filters applied", list(self.filter_dict.keys()))
        mlflow.log_param("target_variable", pollutant)
        mlflow.log_param("country", country)
        mlflow.log_param("source", source)

    def cohort_builder(
        self,
        cohort_type,
        train_validation_dict,
        filter_cols,
        city,
        country,
        source,
        sensor_type,
        pollutant,
        local_data,
    ) -> pd.DataFrame:
        date_tup_list = list(train_validation_dict[f"{cohort_type}"])

        for index, date_tuple in enumerate(date_tup_list):
            if source == "openaq-aws":
                df = self.execute_for_openaq_aws(
                    date_tuple,
                    city,
                    country,
                    pollutant,
                    sensor_type,
                    local_data,
                )
            elif source == "openaq-api":
                df = self.execute_for_openaq_api(
                    date_tuple,
                    city,
                    country,
                    pollutant,
                    sensor_type,
                    local_data,
                )
            else:
                continue
            df["train_validation_set"] = index
            df["cohort"] = f"{index}_{date_tuple[0]}_{date_tuple[1]}"
            df["cohort_type"] = f"{cohort_type}"
            if df.empty:
                logging.info(
                    f"No openaq data found for {date_tuple[0]}_{date_tuple[1]} time window"
                )
            filtered_df = (
                Preprocess()
                .from_options(list(self.filter_dict.keys()))
                .execute(df, source)
            ).reset_index(drop=True)
            engine = get_dbengine(
                os.getenv("PGDATABASE"),
                os.getenv("PGHOST"),
                os.getenv("PGPORT"),
                os.getenv("PGUSER"),
                os.getenv("PGPASSWORD"),
            )
            self._results_to_db(filtered_df, engine, city)

    def execute_for_openaq_aws(
        self, date_tuple, city, country, pollutant, sensor_type, local_data
    ):
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
                WHERE parameter='{target_variable}'
                AND {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}';""".format(
                table=self.table_name,
                target_variable=self.target_variable,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
            )
        elif city:
            query = """SELECT DISTINCT *
                FROM {table}
                WHERE parameter='{target_variable}'
                AND city='{city}'
                AND {date_col}
                BETWEEN '{start_date}'
                AND '{end_date}';""".format(
                table=self.table_name,
                target_variable=self.target_variable,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
                city=city,
            )
        else:
            query = """SELECT DISTINCT *
                FROM {table}
                WHERE parameter='{target_variable}' AND country='{country}'
                AND {date_col} BETWEEN '{start_date}' AND '{end_date}' LIMIT 1000;""".format(
                table=self.table_name,
                target_variable=self.target_variable,
                date_col=self.date_col,
                start_date=date_tuple[0],
                end_date=date_tuple[1],
                country=country,
            )
        df = self.build_response_from_aws(params, query)
        if local_data:
            df = self._get_local_data(date_tuple, local_data, df)
        return df

    def execute_for_openaq_api(
        self, date_tuple, city, country, pollutant, sensor_type, local_data
    ):
        if pollutant:
            self.target_variable = pollutant
        if country == "WO":
            url = """https://api.openaq.org/v2/measurements?date_from={date_from}&date_to={date_to}&limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&order_by=datetime&sensor_type={sensor_type}""".format(
                date_from=date_tuple[0],
                date_to=date_tuple[1],
                pollutant=self.target_variable,
                sensor_type=sensor_type,
            )
        elif city:
            url = """https://api.openaq.org/v2/measurements?date_from={date_from}&date_to={date_to}&limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&city={city}&order_by=datetime&sensorType={sensor_type}""".format(
                date_from=date_tuple[0],
                date_to=date_tuple[1],
                pollutant=self.target_variable,
                city=city,
                sensor_type=sensor_type,
            )
        else:
            if pollutant == "pm25":
                parameter_id = 2
                url = """https://api.openaq.org/v2/measurements?date_from={date_from}&date_to={date_to}&parameter_id={parameter_id}&country={country}""".format(
                    date_from=date_tuple[0],
                    date_to=date_tuple[1],
                    country=country,
                    parameter_id=parameter_id,
                )
        df = api_response_to_df(url)
        if local_data:
            df = self._get_local_data(date_tuple, local_data, df)
        return df

    def _get_local_data(self, date_tuple, local_data, df):
        local_df = get_data(f"""SELECT * FROM "{local_data}" """)
        if not local_df.empty:
            local_df = self.create_cohort_from_local_data(local_df, date_tuple)
            df = pd.concat([df, local_df], axis=0).reset_index(drop=True)
            return df
        else:
            logging.info(
                f"No local data found for {date_tuple[0]}_{date_tuple[1]} time window"
            )
            return pd.DataFrame()

    def create_cohort_from_local_data(self, local_data, date_tuple):
        start_utc_datetime = datetime.fromisoformat(
            date_tuple[0].replace("Z", "+00:00")
        ).date()
        end_utc_datetime = datetime.fromisoformat(
            date_tuple[1].replace("Z", "+00:00")
        ).date()
        local_data["utc_date"] = local_data["date"].apply(extract_utc_date)
        filtered_df = local_data[
            (local_data["utc_date"] >= start_utc_datetime)
            & (local_data["utc_date"] <= end_utc_datetime)
        ]
        return filtered_df[
            [
                "locationId",
                "location",
                "city",
                "parameter",
                "value",
                "date",
                "unit",
                "coordinates",
                "country",
                "isMobile",
                "isAnalysis",
                "entity",
                "sensorType",
            ]
        ]

    def _results_to_db(self, filtered_cohorts_df, engine, city):
        if city:
            location = city
        else:
            location = self.country
        write_to_db(
            filtered_cohorts_df,
            engine,
            f"cohorts_{location}",
            "public",
            "append",
        )

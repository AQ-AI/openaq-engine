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
from sqlalchemy import text
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
    """
    Base class for building cohorts from AWS Athena and other sources.

    :param table_name: The name of the table to query data from.
    :type table_name: str
    :param region_name: The AWS region where the data is stored.
    :type region_name: str
    :param bucket: The name of the S3 bucket where results will be stored.
    :type bucket: str
    :param s3_output: The S3 path where output files will be saved.
    :type s3_output: str
    """

    def __init__(
        self, table_name: str, region_name: str, bucket: str, s3_output: str
    ):
        self.table_name = table_name
        self.region_name = region_name
        self.bucket = bucket
        self.s3_output = s3_output

    def build_response_from_aws(self, params, sql_query):
        """
        Execute a query on AWS Athena and return the result as a DataFrame.

        :param params: The parameters for the AWS Athena query.
        :type params: dict
        :param sql_query: The SQL query to execute.
        :type sql_query: str
        :return: The result of the query as a pandas DataFrame.
        :rtype: pd.DataFrame
        """
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
        """
        Extract VarChar values from the query result.

        :param row: A row from the query result.
        :type row: dict
        :return: A list of VarChar values from the row.
        :rtype: list
        """
        return [
            d["VarCharValue"] if "VarCharValue" in d else "{}"
            for d in row["Data"]
        ]


class CohortBuilder(CohortBuilderBase):
    """
    Class for building cohorts for machine learning models.

    :param date_col: The name of the date column in the data.
    :type date_col: str
    :param filter_dict: A dictionary of filters to apply to the data.
    :type filter_dict: dict
    :param target_variable: A list of target variables for the cohort.
    :type target_variable: list
    :param country: The country for which the cohort is being built.
    :type country: str
    :param source: The data source, e.g., 'openaq-aws' or 'openaq-api'.
    :type source: str
    """

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
        """
        Create a CohortBuilder instance from a configuration dataclass.

        :param config: The configuration dataclass.
        :type config: CohortBuilderConfig
        :return: An instance of CohortBuilder.
        :rtype: CohortBuilder
        """
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
        """
        Execute the cohort building process and log the results.

        :param train_validation_dict: A dictionary containing train/validation splits.
        :type train_validation_dict: dict
        :param engine: The database engine to use for saving results.
        :type engine: Any
        :param country: The country for which the cohort is being built.
        :type country: str
        :param source: The data source, e.g., 'openaq-aws' or 'openaq-api'.
        :type source: str
        :param pollutant: The target pollutant variable.
        :type pollutant: str
        """
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
        """
        Retrieve data for cohorts based on predefined timesplits.

        :param cohort_type: The type of cohort (e.g., 'train', 'validation').
        :type cohort_type: str
        :param train_validation_dict: A dictionary containing train/validation splits.
        :type train_validation_dict: dict
        :param filter_cols: Columns to apply filters on.
        :type filter_cols: str
        :param country: The country for which the cohort is being built.
        :type country: str
        :param source: The data source, e.g., 'openaq-aws' or 'openaq-api'.
        :type source: str
        :param pollutant: The target pollutant variable.
        :type pollutant: str
        :return: A DataFrame containing the cohort data.
        :rtype: pd.DataFrame
        """
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
        """
        Execute a query on OpenAQ data stored in AWS Athena.

        :param date_tuple: A tuple of start and end dates.
        :type date_tuple: tuple
        :param country: The country for which the cohort is being built.
        :type country: str
        :param pollutant: The target pollutant variable.
        :type pollutant: str
        :return: The result of the query as a pandas DataFrame.
        :rtype: pd.DataFrame
        """
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
                AND {date_col} BETWEEN '{start_date}' AND '{end_date}';""".format(
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
        """
        Retrieve OpenAQ data using the OpenAQ API.

        :param date_tuple: A tuple of start and end dates.
        :type date_tuple: tuple
        :param country: The country for which the cohort is being built.
        :type country: str
        :param pollutant: The target pollutant variable.
        :type pollutant: str
        :return: The result of the API call as a pandas DataFrame.
        :rtype: pd.DataFrame
        """
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
        local_df = get_data(text(f"""SELECT * FROM "{local_data}" """))
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
                "locationid",
                "location",
                "city",
                "parameter",
                "value",
                "date",
                "unit",
                "coordinates",
                "country",
                "ismobile",
                "isanalysis",
                "entity",
                "sensortype",
            ]
        ]

    def _results_to_db(self, filtered_cohorts_df, engine, *args):
        """
        Write the filtered cohort results to the database.

        :param filtered_cohorts_df: The DataFrame containing filtered cohorts.
        :type filtered_cohorts_df: pd.DataFrame
        :param engine: The database engine to use for saving results.
        :type engine: Any
        :param args: Additional arguments, expecting city as the first argument.
        """
        city = args[0] if len(args) > 0 else None
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

import logging
from abc import ABC
from datetime import datetime
from typing import Any, Dict, List, Tuple

import mlflow
from dateutil.relativedelta import relativedelta

from config.model_settings import TimeSplitterConfig
from openaq_engine.src.utils.utils import (
    query_results_from_api,
    query_results_from_aws,
)

logging.basicConfig(level=logging.INFO)


class TimeSplitterBase(ABC):
    """
    Base class for splitting time windows for data preprocessing.

    :param date_col: The name of the date column in the data.
    :type date_col: str
    :param table_name: The name of the table to query data from.
    :type table_name: str
    :param database: The name of the database.
    :type database: str
    :param region_name: The AWS region where the data is stored.
    :type region_name: str
    :param bucket: The name of the S3 bucket where results will be stored.
    :type bucket: str
    :param s3_output: The S3 path where output files will be saved.
    :type s3_output: str
    """

    def __init__(
        self,
        date_col: str,
        table_name: str,
        database: str,
        region_name: str,
        bucket: str,
        s3_output: str,
    ):
        self.date_col = date_col
        self.table_name = table_name
        self.database = database
        self.region_name = region_name
        self.bucket = bucket
        self.s3_output = s3_output

    def create_end_date_from_aws(
        self,
        params: Dict[str, Any],
        country_info: List[str],
        pollutant: str,
        latest_date: str,
    ) -> datetime:
        """
        Query AWS Athena to retrieve the end date of data given specific filters.

        :param params: The parameters for the AWS Athena query.
        :type params: dict
        :param country_info: The country information or "WO" for worldwide.
        :type country_info: list
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param latest_date: The latest date to consider in the query.
        :type latest_date: str
        :return: The end date of the data as a datetime object.
        :rtype: datetime
        """
        if pollutant:
            self.target_variable = pollutant
        if not latest_date:
            latest_date = "DATE(NOW())"
        if country_info == "WO":
            sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            ORDER BY {date_col} DESC limit 1;""".format(
                table=self.table_name,
                date_col=self.date_col,
                target_variable=self.target_variable,
                latest_date=latest_date,
            )
        else:
            sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            AND country='{country}'
            ORDER BY {date_col} DESC limit 1;""".format(
                table=self.table_name,
                date_col=self.date_col,
                target_variable=self.target_variable,
                country=country_info,
                latest_date=latest_date,
            )
        response_query_result = self.build_response_from_aws(params, sql_query)

        return datetime.strptime(
            f"{response_query_result}", "%Y-%m-%d %H:%M:%S.000 UTC"
        ).date()

    def create_start_date_from_aws(
        self,
        params: Dict[str, Any],
        country_info: List[str],
        pollutant: str,
        latest_date: str,
    ) -> datetime:
        """
        Query AWS Athena to retrieve the start date of data given specific filters.

        :param params: The parameters for the AWS Athena query.
        :type params: dict
        :param country_info: The country information or "WO" for worldwide.
        :type country_info: list
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param latest_date: The latest date to consider in the query.
        :type latest_date: str
        :return: The start date of the data as a datetime object.
        :rtype: datetime
        """
        if pollutant:
            self.target_variable = pollutant
        if not latest_date:
            latest_date = "DATE(NOW())"
        if country_info == "WO":
            sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            ORDER BY {date_col} ASC limit 1;""".format(
                table=self.table_name,
                date_col=self.date_col,
                target_variable=self.target_variable,
                latest_date=latest_date,
            )
        else:
            sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime
            FROM {table} WHERE parameter='{target_variable}'
            AND from_iso8601_timestamp({date_col}) <= {latest_date}
            AND country='{country}'
            ORDER BY {date_col} ASC limit 1;""".format(
                table=self.table_name,
                date_col=self.date_col,
                target_variable=self.target_variable,
                country=country_info,
                latest_date=latest_date,
            )
        response_query_result = self.build_response_from_aws(params, sql_query)

        return datetime.strptime(
            f"{response_query_result}", "%Y-%m-%d %H:%M:%S.000 UTC"
        ).date()

    def build_response_from_aws(
        self, params: Dict[str, Any], sql_query: str
    ) -> str:
        """
        Execute the SQL query on AWS Athena and retrieve the results.

        :param params: The parameters for the AWS Athena query.
        :type params: dict
        :param sql_query: The SQL query to execute.
        :type sql_query: str
        :return: The result as a string value.
        :rtype: str
        """
        response_query_result = query_results_from_aws(params, sql_query)
        response_query_result["ResultSet"]["Rows"][0]
        rows = response_query_result["ResultSet"]["Rows"][1:]
        for row in rows:
            return self._get_var_char_values(row)

    def _get_var_char_values(self, d: Dict[str, Any]) -> str:
        """
        Extract the VarChar value from the data dictionary.

        :param d: A dictionary containing the data.
        :type d: dict
        :return: The VarChar value as a string.
        :rtype: str
        """
        for obj in d["Data"]:
            if obj["VarCharValue"]:
                return obj["VarCharValue"]
            else:
                pass

    def create_end_date_from_openaq_api(
        self, country: str, pollutant: str, latest_date: str
    ) -> datetime:
        """
        Retrieve the end date of data using the OpenAQ API.

        :param country: The country code for filtering the data.
        :type country: str
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param latest_date: The latest date to consider in the query.
        :type latest_date: str
        :return: The end date of the data as a datetime object.
        :rtype: datetime
        """
        if country == "WO":
            url = f"https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&order_by=lastUpdated&dumpRaw=false"
        else:
            url = f"https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&country={country}&order_by=lastUpdated&dumpRaw=false"

        headers = {"accept": "application/json"}
        response = query_results_from_api(headers, url)
        response_json = response.json()

        if "results" in response_json and response_json["results"]:
            return datetime.strptime(
                response_json["results"][0]["lastUpdated"],
                "%Y-%m-%dT%H:%M:%S+00:00",
            ).date()
        else:
            return (
                datetime.date.today()
            )  # fallback in case of unexpected response

    def create_start_date_from_openaq_api(
        self, country: str, pollutant: str
    ) -> datetime:
        """
        Retrieve the start date of data using the OpenAQ API.

        :param country: The country code for filtering the data.
        :type country: str
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :return: The start date of the data as a datetime object.
        :rtype: datetime
        """
        if country == "WO":
            url = f"https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=asc&parameter={pollutant}&radius=1000&order_by=firstUpdated&dumpRaw=false"
        else:
            url = f"https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=asc&parameter={pollutant}&radius=1000&country={country}&order_by=firstUpdated&dumpRaw=false"

        headers = {"accept": "application/json"}
        response = query_results_from_api(headers, url)
        response_json = response.json()

        return datetime.strptime(
            response_json["results"][0]["firstUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        ).date()


class TimeSplitter(TimeSplitterBase):
    """
    Class for splitting time windows for training and validation.

    :param time_window_length: The length of each time window in months.
    :type time_window_length: int
    :param within_window_sampler: The number of months to sample within each window.
    :type within_window_sampler: int
    :param window_count: The number of time windows to generate.
    :type window_count: int
    :param train_validation_dict: A dictionary to store training and validation time windows.
    :type train_validation_dict: dict
    :param target_variable: The target variable to consider for splitting.
    :type target_variable: str
    :param country: The country code for filtering the data.
    :type country: str
    :param source: The source of the data, e.g., 'openaq-aws' or 'openaq-api'.
    :type source: str
    """

    def __init__(
        self,
        time_window_length: int,
        within_window_sampler: int,
        window_count: int,
        train_validation_dict: Dict[str, List[Any]],
        target_variable: str,
        country: str,
        source: str,
    ) -> None:
        self.time_window_length = time_window_length
        self.within_window_sampler = within_window_sampler
        self.window_count = window_count
        self.train_validation_dict = train_validation_dict
        self.target_variable = target_variable
        self.country = country
        self.source = source

        super().__init__(
            TimeSplitterConfig.DATE_COL,
            TimeSplitterConfig.TABLE_NAME,
            TimeSplitterConfig.DATABASE,
            TimeSplitterConfig.REGION,
            TimeSplitterConfig.S3_BUCKET,
            TimeSplitterConfig.S3_OUTPUT,
        )

    @classmethod
    def from_dataclass_config(
        cls, config: TimeSplitterConfig
    ) -> "TimeSplitter":
        """
        Create a TimeSplitter instance from a configuration dataclass.

        :param config: The configuration dataclass.
        :type config: TimeSplitterConfig
        :return: An instance of TimeSplitter.
        :rtype: TimeSplitter
        """
        return cls(
            time_window_length=config.TIME_WINDOW_LENGTH,
            within_window_sampler=config.WITHIN_WINDOW_SAMPLER,
            window_count=config.WINDOW_COUNT,
            train_validation_dict=config.TRAIN_VALIDATION_DICT,
            target_variable=config.TARGET_VARIABLE,
            country=config.COUNTRY,
            source=config.SOURCE,
        )

    def execute(
        self, country: str, source: str, pollutant: str, date: str
    ) -> Dict[str, List[Any]]:
        """
        Generate a list of dates between start and end dates for each time window.

        :param country: The country code for filtering the data.
        :type country: str
        :param source: The source of the data, e.g., 'openaq-aws' or 'openaq-api'.
        :type source: str
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param date: The reference date for generating time windows.
        :type date: str
        :return: A dictionary containing training and validation time windows.
        :rtype: dict
        """
        mlflow.log_param("time_window_length", self.time_window_length)
        mlflow.log_param("within_window_sampler", self.within_window_sampler)
        mlflow.log_param("window_count", self.window_count)
        mlflow.log_param("target_variable", self.target_variable)
        mlflow.log_param("country", self.country)
        mlflow.log_param("source", self.source)

        window_no = 0
        params = {
            "region": str(self.region_name),
            "database": str(self.database),
            "bucket": str(self.bucket),
            "path": f"{self.s3_output}",
        }
        if source == "openaq-aws":
            end_date, start_date = self.execute_for_openaq_aws(
                params, country, pollutant, date
            )
        if source == "openaq-api":
            end_date, start_date = self.execute_for_openaq_api(
                country, pollutant, date
            )
        while window_no < self.window_count:
            window_start_date, window_end_date = self.get_validation_window(
                end_date, window_no
            )
            logging.info(
                f"""Getting cohort between {window_start_date}
                and {window_end_date}"""
            )
            if window_start_date < start_date:
                logging.warning(
                    f"""Date: {window_start_date} is earlier than
                    the first date within data: {start_date}"""
                )
                window_no += 1
            else:
                self.train_validation_dict["validation"] += [
                    (
                        window_start_date,
                        window_end_date,
                    )
                ]
                self.train_validation_dict["training"] += [
                    (start_date, window_start_date)
                ]
                window_no += 1
        mlflow.log_params(self.train_validation_dict)
        return self.train_validation_dict

    def execute_for_openaq_aws(
        self,
        params: Dict[str, Any],
        country: str,
        pollutant: str,
        latest_date: str,
    ) -> Tuple[datetime, datetime]:
        """
        Execute the time window split using AWS Athena.

        :param params: The parameters for the AWS Athena query.
        :type params: dict
        :param country: The country code for filtering the data.
        :type country: str
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param latest_date: The latest date to consider in the query.
        :type latest_date: str
        :return: The end date and start date as datetime objects.
        :rtype: tuple
        """
        end_date = self.create_end_date_from_aws(
            params, country, pollutant, latest_date
        )
        start_date = self.create_start_date_from_aws(
            params, country, pollutant, latest_date
        )
        return end_date, start_date

    def execute_for_openaq_api(
        self, country: str, pollutant: str, latest_date: str
    ) -> Tuple[datetime, datetime]:
        """
        Execute the time window split using the OpenAQ API.

        :param country: The country code for filtering the data.
        :type country: str
        :param pollutant: The pollutant to filter the data on.
        :type pollutant: str
        :param latest_date: The latest date to consider in the query.
        :type latest_date: str
        :return: The end date and start date as datetime objects.
        :rtype: tuple
        """
        end_date = self.create_end_date_from_openaq_api(
            country, pollutant, latest_date
        )
        start_date = self.create_start_date_from_openaq_api(country, pollutant)
        return end_date, start_date

    def get_validation_window(
        self, end_date: datetime, window_no: int
    ) -> Tuple[datetime, datetime]:
        """
        Get the start and end date of each validation window.

        :param end_date: The end date of the time window.
        :type end_date: datetime
        :param window_no: The window number.
        :type window_no: int
        :return: The start and end date of the validation window.
        :rtype: tuple
        """
        window_start_date = self._get_start_time_windows(end_date, window_no)
        window_end_date = self._get_end_time_windows(window_start_date)
        return window_start_date, window_end_date

    def _get_start_time_windows(
        self, window_date: datetime, window_no: int
    ) -> datetime:
        """
        Get the start date of a window based on the window length.

        :param window_date: The reference date for the time window.
        :type window_date: datetime
        :param window_no: The window number.
        :type window_no: int
        :return: The start date of the time window.
        :rtype: datetime
        """
        return window_date - relativedelta(
            months=+window_no * self.time_window_length
            + self.within_window_sampler
        )

    def _get_end_time_windows(self, window_start_date: datetime) -> datetime:
        """
        Get the end date of a window based on the start date and window length.

        :param window_start_date: The start date of the time window.
        :type window_start_date: datetime
        :return: The end date of the time window.
        :rtype: datetime
        """
        return window_start_date + relativedelta(
            months=+self.within_window_sampler
        )

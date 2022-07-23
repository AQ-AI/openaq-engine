from abc import ABC
from typing import Any, Dict, List
import logging
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta

import boto3
import pandas as pd

from config.model_settings import TimeSplitterConfig
from src.utils.utils import read_csv, query_results

logging.basicConfig(level=logging.INFO)


class TimeSplitterBase(ABC):
    def __init__(
        self,
        date_col: str,
        table_name: str,
        database: str,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket: str,
        s3_output: str,
        resource: boto3.resource,
    ):

        self.date_col = date_col
        self.table_name = table_name
        self.database = database
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.s3_output = s3_output
        self.resource = resource

    def _obtain_df_from_s3(self, filepath):
        print(filepath)

        s3 = boto3.client("s3", region_name=self.region_name)

        obj = s3.get_object(Bucket=self.bucket, Key=filepath)

        # response = self.resource.Bucket(self.bucket).Object(key=filepath).get()

        return read_csv(obj)

    def create_end_date(self, params) -> pd.DataFrame:
        sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime 
        FROM {table} WHERE parameter='pm25' 
        AND from_iso8601_timestamp({date_col}) <= DATE(NOW()) 
        ORDER BY {date_col} DESC limit 1;""".format(
            table=self.table_name, date_col=self.date_col
        )
        response_query_result = self._build_response(params, sql_query)
        print(response_query_result)

        return datetime.strptime(
            f"{response_query_result}", "%Y-%m-%d %H:%M:%S.000 UTC"
        ).date()

    def create_start_date(self, params: Dict[str, Any]) -> datetime:
        sql_query = """SELECT from_iso8601_timestamp({date_col}) AS datetime 
        FROM {table} WHERE parameter='pm25' 
        AND from_iso8601_timestamp({date_col}) <= DATE(NOW()) 
        ORDER BY {date_col} ASC limit 1;""".format(
            table=self.table_name, date_col=self.date_col
        )

        response_query_result = self._build_response(params, sql_query)
        print(response_query_result)

        return datetime.strptime(
            f"{response_query_result}", "%Y-%m-%d %H:%M:%S.000 UTC"
        ).date()

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


class TimeSplitter(TimeSplitterBase):
    def __init__(
        self,
        time_window_length: int,
        within_window_sampler: int,
        window_count: int,
        train_validation_dict: Dict[str, List[Any]],
    ) -> None:
        self.time_window_length = time_window_length
        self.within_window_sampler = within_window_sampler
        self.window_count = window_count
        self.train_validation_dict = train_validation_dict
        super().__init__(
            TimeSplitterConfig.DATE_COL,
            TimeSplitterConfig.TABLE_NAME,
            TimeSplitterConfig.DATABASE,
            TimeSplitterConfig.REGION,
            TimeSplitterConfig.AWS_ACCESS_KEY,
            TimeSplitterConfig.AWS_SECRET_ACCESS_KEY,
            TimeSplitterConfig.S3_BUCKET,
            TimeSplitterConfig.S3_OUTPUT,
            TimeSplitterConfig.RESOURCE,
        )

    @classmethod
    def from_dataclass_config(cls, config: TimeSplitterConfig) -> "TimeSplitter":
        return cls(
            time_window_length=config.TIME_WINDOW_LENGTH,
            within_window_sampler=config.WITHIN_WINDOW_SAMPLER,
            window_count=config.WINDOW_COUNT,
            train_validation_dict=config.TRAIN_VALIDATION_DICT,
        )

    def execute(
        self,
    ):
        """
        Input
        ----
        creates list of dates between start and end date of each time
        window within a given window time length and a given number of months to sample
        ----
        The start and end dates for each time window
        """
        window_no = 0
        params = {
            "region": self.region_name,
            "database": self.database,
            "bucket": self.bucket,
            "path": f"{self.s3_output}/max_date",
        }
        end_date = self.create_end_date(params)
        start_date = self.create_start_date(params)

        while window_no < self.window_count:
            window_start_date, window_end_date = self.get_validation_window(
                end_date, window_no
            )

            self.train_validation_dict["validation"] += [
                (
                    window_start_date,
                    window_end_date,
                )
            ]
            self.train_validation_dict["training"] += [(start_date, window_start_date)]
            window_no += 1
        return self.train_validation_dict

    def get_validation_window(self, end_date, window_no):
        """Gets start and end date of each training window"""
        window_start_date = self._get_start_time_windows(end_date, window_no)
        window_end_date = self._get_end_time_windows(window_start_date)
        return window_start_date, window_end_date

    def _get_start_time_windows(self, window_date, window_no):
        """Gets start date of window based on the window length and the number of sample
        months used in the window"""
        return window_date - relativedelta(
            months=+window_no * self.time_window_length + self.within_window_sampler
        )

    def _get_end_time_windows(self, window_start_date):
        """Gets end date of window based on the window length and the number of sample
        months used in the window"""
        return window_start_date + relativedelta(months=+self.within_window_sampler)

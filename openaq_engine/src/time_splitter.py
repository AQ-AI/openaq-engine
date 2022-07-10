from abc import ABC
from typing import Any, Dict
import logging
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
from joblib import Parallel, delayed

import boto3
import pandas as pd

from config.model_settings import TimeSplitterConfig
from src.utils.utils import read_csv

logging.basicConfig(level=logging.INFO)


class TimeSplitterBase(ABC):
    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket: str,
        s3_output: str,
        resource: boto3.resource,
    ):
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.s3_output = s3_output
        self.resource = resource

    def _obtain_df_from_s3(self, filename):

        response = self.resource.Bucket(self.bucket).Object(key=filename).get()

        return read_csv(io.BytesIO(response["Body"].read()))

    def get_s3_file_path_list(self):
        csv_filetype = ".csv"
        my_bucket = self.resource.Bucket(self.bucket)
        csv_list = []
        for object_summary in my_bucket.objects.filter(Prefix="pm25-month/"):
            if object_summary.key.endswith(csv_filetype):
                csv_list.append(object_summary.key)
        print(csv_list)
        return csv_list


class TimeSplitter(TimeSplitterBase):
    def __init__(
        self,
        date_col: str,
        table_name: str,
        time_window_length: int,
        within_window_sampler: int,
        window_count: int,
    ) -> None:
        self.date_col = date_col
        self.table_name = table_name
        self.time_window_length = time_window_length
        self.within_window_sampler = within_window_sampler
        self.window_count = window_count
        super().__init__(
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
            date_col=config.DATE_COL,
            table_name=config.TABLE_NAME,
            time_window_length=config.TIME_WINDOW_LENGTH,
            within_window_sampler=config.WITHIN_WINDOW_SAMPLER,
            window_count=config.WINDOW_COUNT,
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
        self.get_s3_file_path_list()
        # window_no = 0
        # end_date = self.create_end_date(self.date_col, self.table_name)

        # training_validation_date_list = []
        # while window_no <= self.window_count:

        #     window_start_date, window_end_date = self.get_time_window(
        #         end_date, window_no
        #     )
        #     training_validation_date_list.append((window_start_date, window_end_date))
        #     window_no += 1
        # return training_validation_date_list

    def create_end_date(self, *args: Any) -> pd.DataFrame:
        sql_query = """SELECT MAX("{date_col}") FROM "{table}";""".format(
            table=self.table_name, date_col=self.date_col
        )
        # latest_date_df = self.obtain_data_from_s3(self.filename)
        # latest_date_string = latest_date_df.values[0][0]  # Accessing timestamp string
        # return datetime.strptime(f"{latest_date_string}", "%Y-%m-%dT%H:%M:%S.000000000")

    def create_start_date(self, end_date):
        start_date = end_date - relativedelta(months=+self.time_window_length)
        return start_date

    def get_time_window(self, start_date, window_no):
        """Gets start and end date of each training window"""
        window_start_date = self._get_start_time_windows(start_date, window_no)
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


if __name__ == "__main__":
    TimeSplitter().execute()

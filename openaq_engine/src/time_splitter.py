import json
import logging
from abc import ABC
from datetime import datetime
from typing import Any, Dict, List

from dateutil.relativedelta import relativedelta
from src.utils.utils import query_results_from_api, query_results_from_aws

from config.model_settings import TimeSplitterConfig

logging.basicConfig(level=logging.INFO)


class TimeSplitterBase(ABC):
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
        params,
        country_info: List[str],
        pollutant: str,
        latest_date: str,
    ) -> datetime:
        """Build SQL query to query AWS Athena and retrieve
        end date of data given specific filters."""
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
                latest_date=latest_date.isoformat(),
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
                latest_date=latest_date.isoformat(),
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
        """Build SQL query to query AWS Athena and retrieve
        start date of data given specific filters."""
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
                latest_date=latest_date.isoformat(),
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
                latest_date=latest_date.isoformat(),
            )
        if self.source == "openaq-aws":
            response_query_result = self.build_response_from_aws(
                params, sql_query
            )
        return datetime.strptime(
            f"{response_query_result}", "%Y-%m-%d %H:%M:%S.000 UTC"
        ).date()

    def build_response_from_aws(self, params, sql_query):
        response_query_result = query_results_from_aws(params, sql_query)
        response_query_result["ResultSet"]["Rows"][0]
        rows = response_query_result["ResultSet"]["Rows"][1:]
        for row in rows:
            return self._get_var_char_values(row)

    def _get_var_char_values(self, d):
        for obj in d["Data"]:
            if obj["VarCharValue"]:
                return obj["VarCharValue"]
            else:
                pass

    def create_end_date_from_openaq_api(self, country, pollutant, latest_date):
        if country == "WO":
            url = """https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&order_by=lastUpdated&dumpRaw=false""".format(
                pollutant=pollutant
            )
        else:
            url = """https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&parameter={pollutant}&radius=1000&country_id={country}&order_by=lastUpdated&dumpRaw=false""".format(
                country=country, pollutant=pollutant
            )

        headers = {"accept": "application/json"}
        response = query_results_from_api(headers, url)
        return datetime.strptime(
            json.loads(response)["results"][0]["lastUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        ).date()

    def create_start_date_from_openaq_api(
        self,
        country,
        pollutant,
    ):
        if country == "WO":
            url = """https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=asc&parameter={pollutant}&radius=1000&order_by=firstUpdated&dumpRaw=false""".format(
                pollutant=pollutant
            )
        else:
            url = """https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=asc&parameter={pollutant}&radius=1000&country_id={country}&order_by=firstUpdated&dumpRaw=false""".format(
                country=country, pollutant=pollutant
            )

        headers = {"accept": "application/json"}

        response = query_results_from_api(headers, url)

        return datetime.strptime(
            json.loads(response)["results"][0]["firstUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        ).date()


class TimeSplitter(TimeSplitterBase):
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
        return cls(
            time_window_length=config.TIME_WINDOW_LENGTH,
            within_window_sampler=config.WITHIN_WINDOW_SAMPLER,
            window_count=config.WINDOW_COUNT,
            train_validation_dict=config.TRAIN_VALIDATION_DICT,
            target_variable=config.TARGET_VARIABLE,
            country=config.COUNTRY,
            source=config.SOURCE,
        )

    def execute(self, country, source, pollutant, date):
        """
        Input
        ----
        creates list of dates between start and end date of each time
        window within a given window time length and a given number of
        months to sample
        ----
        The start and end dates for each time window
        """
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
                    f"""Date: {window_start_date.date()} is earlier than
                    the first date within data: {start_date.date()}"""
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
        return self.train_validation_dict

    def execute_for_openaq_aws(self, params, country, pollutant, latest_date):
        end_date = self.create_end_date_from_aws(
            params, country, pollutant, latest_date
        )
        start_date = self.create_start_date_from_aws(
            params, country, pollutant, latest_date
        )
        return end_date, start_date

    def execute_for_openaq_api(self, country, pollutant, latest_date):
        end_date = self.create_end_date_from_openaq_api(
            country, pollutant, latest_date
        )
        start_date = self.create_start_date_from_openaq_api(country, pollutant)
        return end_date, start_date

        return

    def get_validation_window(self, end_date, window_no):
        """Gets start and end date of each training window"""
        window_start_date = self._get_start_time_windows(end_date, window_no)
        window_end_date = self._get_end_time_windows(window_start_date)
        return window_start_date, window_end_date

    def _get_start_time_windows(self, window_date, window_no):
        """Gets start date of window based on the window length and
        the number of sample months used in the window"""
        return window_date - relativedelta(
            months=+window_no * self.time_window_length
            + self.within_window_sampler
        )

    def _get_end_time_windows(self, window_start_date):
        """Gets end date of window based on the window length
        and the number of sample months used in the window"""
        return window_start_date + relativedelta(
            months=+self.within_window_sampler
        )

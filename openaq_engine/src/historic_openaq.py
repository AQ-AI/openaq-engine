from datetime import datetime
from config.model_settings import HistoricOpenAQConfig
from src.utils.utils import query_results


class HistoricOpenAQ:
    def __init__(
        self,
        database: str,
        region: str,
        s3_output: str,
        s3_bucket: str,
        dates: datetime.date,
    ) -> None:
        self.database = database
        self.region = region
        self.s3_output = s3_output
        self.s3_bucket = s3_bucket
        self.dates = dates

    @classmethod
    def from_dataclass_config(
        cls, config: HistoricOpenAQConfig
    ) -> "HistoricOpenAQ":

        return cls(
            database=config.DATABASE,
            region=config.REGION,
            s3_output=config.S3_OUTPUT,
            s3_bucket=config.S3_BUCKET,
            dates=config.DATES,
        )

    def execute(self):
        for month in self.dates:
            print(self.get_results(month))

    def get_results(self, month, wait=True):
        params = {
            "region": self.region,
            "database": self.database,
            "bucket": self.s3_bucket,
            "path": f"{self.s3_output}/{str(month.strftime('%Y-%m-%d'))}",
        }
        first_day_of_month = self._get_first_day_of_month(month.date())

        query = f"SELECT * FROM openaq WHERE PARAMETER = 'pm25' and date.local between '{str(first_day_of_month)}' and '{str(month)}' and value >= 0 limit 10;"
        response_query_result, location = query_results(
            params,
            query,
        )

        if len(response_query_result["ResultSet"]["Rows"]) > 1:
            header = response_query_result["ResultSet"]["Rows"][0]
            rows = response_query_result["ResultSet"]["Rows"][1:]

            header = [obj["VarCharValue"] for obj in header["Data"]]
            result = [
                dict(zip(header, self.get_var_char_values(row)))
                for row in rows
            ]

            return location, result
        else:
            return location, None

    def get_var_char_values(self, d):
        for obj in d["Data"]:
            if obj["VarCharValue"]:
                return obj["VarCharValue"]
            else:
                pass

    def _get_first_day_of_month(self, date_time):
        return datetime(date_time.year, date_time.month, 1)

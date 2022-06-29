import time
import calendar
from datetime import datetime, timedelta, date
from joblib import Parallel, delayed
from config.model_settings import HistoricOpenAQConfig


class HistoricOpenAQ:
    def __init__(
        self,
        database: str,
        region: str,
        s3_output: str,
        s3_bucket: str,
        dates: datetime.date,
        time_aggregation: int,
    ) -> None:
        self.database = database
        self.region = region
        self.s3_output = s3_output
        self.s3_bucket = s3_bucket
        self.dates = dates
        self.time_aggregation = time_aggregation

    @classmethod
    def from_dataclass_config(cls, config: HistoricOpenAQConfig) -> "HistoricOpenAQ":

        return cls(
            database=config.DATABASE,
            region=config.REGION,
            s3_output=config.S3_OUTPUT,
            s3_bucket=config.S3_BUCKET,
            dates=config.DATES,
            time_aggregation=config.TIME_AGGREGATION,
        )

    def execute(self, session):
        # Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
        #     delayed(self.query_results)(session, params, date) for date in self.dates
        # )
        for date in self.dates:
            self.query_results(session, date)

    def query_results(self, session, date, wait=True):
        print(date.strftime("%Y-%m-%d"))
        params = {
            "region": self.region,
            "database": self.database,
            "bucket": self.s3_bucket,
            "path": f"{self.s3_output}/{str(date.strftime('%Y-%m-%d'))}",
        }
        next_time = date + timedelta(weeks=self.time_aggregation)

        client = session.client("athena", params["region"])
        query = f"SELECT * FROM openaq WHERE PARAMETER = 'pm25' and date.local between '{str(date)}' and '{str(next_time)}' and value >= 0;"
        print(query)
        ## This function executes the query and returns the query execution ID
        response_query_execution_id = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "default"},
            ResultConfiguration={
                "OutputLocation": "s3://"
                + params["bucket"]
                + "/"
                + params["path"]
                + "/"
            },
        )

        if not wait:
            return response_query_execution_id["QueryExecutionId"]
        else:
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id["QueryExecutionId"]
            )
            status = "RUNNING"
            iterations = 360000  # 30 mins

            while iterations > 0:
                iterations = iterations - 1
                response_get_query_details = client.get_query_execution(
                    QueryExecutionId=response_query_execution_id["QueryExecutionId"]
                )
                status = response_get_query_details["QueryExecution"]["Status"]["State"]

                if (status == "FAILED") or (status == "CANCELLED"):
                    failure_reason = response_get_query_details["QueryExecution"][
                        "Status"
                    ]["StateChangeReason"]
                    print(failure_reason)
                    return False, False

                elif status == "SUCCEEDED":
                    location = response_get_query_details["QueryExecution"][
                        "ResultConfiguration"
                    ]["OutputLocation"]

                    ## Function to get output results
                    response_query_result = client.get_query_results(
                        QueryExecutionId=response_query_execution_id["QueryExecutionId"]
                    )
                    result_data = response_query_result["ResultSet"]

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
            else:
                time.sleep(5)

            return False

    def get_var_char_values(self, d):
        for obj in d["Data"]:
            if obj["VarCharValue"]:
                return obj["VarCharValue"]
            else:
                pass

    def _get_last_day_of_month(self):
        last_day = datetime.date(
            date.year, date.month, calendar.monthrange(date.year, date.month)[1]
        )

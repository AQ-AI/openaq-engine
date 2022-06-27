import os
from dataclasses import field
from typing import Any, Dict, List, Sequence

from pydantic.dataclasses import dataclass
from utils.utils import date_range

import pandas as pd


@dataclass
class HistoricOpenAQConfig:
    # athena constant
    DATABASE = os.getenv("DB_NAME_OPENAQ")
    TABLE = "openaq"
    REGION = "us-east-1"
    # S3 constant
    S3_OUTPUT = os.getenv("S3_OUTPUT_OPENAQ")
    S3_BUCKET = os.getenv("S3_BUCKET_OPENAQ")

    # number of retries

    # query constant
    PARAMETER = "pm25"

    DATES = pd.date_range("2021-01-01", "2021-02-01", freq="D")

import datetime
import re
from typing import List, Sequence, Tuple

import ee
from ee.ee_exception import EEException
from googleapiclient.errors import HttpError
from joblib import Parallel, delayed

from config.model_settings import EEConfig
from src.utils.utils import get_data


class EEData:
    def __init__(
        self,
        date_col: int,
        table_name: int,
        all_satellites: zip(List[str], List[str]),
        bucket_name: str,
    ):

        self.date_col = date_col
        self.table_name = table_name
        self.all_satellites = all_satellites
        self.bucket_name = bucket_name

    @classmethod
    def from_dataclass_config(cls, config: EEConfig) -> "EEData":
        return cls(
            date_col=config.DATE_COL,
            table_name=config.TABLE_NAME,
            all_satellites=config.ALL_SATELITTES,
            bucket_name=config.BUCKET_NAME,
        )

    def execute(self, engine, save_images):
        start_date_query = """SELECT {date_col} AS datetime
        FROM {table} ORDER BY {date_col} ASC limit 1;""".format(
            table=self.table_name,
            date_col=self.date_col,
        )
        end_date_query = """SELECT {date_col} AS datetime
        FROM {table} ORDER BY {date_col} DESC limit 1;""".format(
            table=self.table_name,
            date_col=self.date_col,
        )
        ee.Authenticate()
        end_date = get_data(end_date_query)
        start_date = get_data(start_date_query)
        s_datetime = self._generate_date(str(start_date["datetime"][0]))
        e_datetime = self._generate_date(str(end_date["datetime"][0]))
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_collection)(
                collection, image_bands, s_datetime, e_datetime, save_images
            )
            for collection, image_bands in self.all_satellites
        )

    def execute_for_collection(
        self, collection, image_bands, s_datetime, e_datetime, save_images
    ):
        """
        Input
        ----
        Takes the name of a satellite image collection and the bands
        (fields) to extract data for, and for a given date range writes the
        extracted satellite `.tiff` files to a google file structure.

        Arguments:
        ----
        collection:
            A str of satellite to query
        image_bands:
            A list of bands captured for each satellite
        s_datetime:
            the start date to extract satellite images for
        e_datetime:
            the end date to extract satellite images for
        save_images:
            a boolean flag whether to write satellite data to google storage
        """
        ee.Initialize()
        collection = (
            ee.ImageCollection(collection)
            .select(image_bands)
            .filterDate(s_datetime, e_datetime)
        )
        print(collection)
        down_args = {
            "image": collection,
            "bucket": self.bucket_name,
            "description": f"{collection}_{s_datetime}_{e_datetime}",
            "scale": 30,
        }

        if save_images is True:
            task = ee.batch.Export.image.toCloudStorage(**down_args)
            task.start()

    def _generate_date(self, date_str) -> datetime.date:
        """Get datetime from string"""
        date = datetime.datetime.strptime(
            re.search("(?<=utc=)(.*)(?=, )", date_str).group(0), "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        return date

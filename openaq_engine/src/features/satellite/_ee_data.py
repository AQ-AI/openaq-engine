import logging
from typing import List, Tuple

import ee
import pandas as pd
from ee.ee_exception import EEException
from geetools import batch
from googleapiclient.errors import HttpError
from joblib import Parallel, delayed
from src.utils.utils import ee_array_to_df, get_data

from config.model_settings import EEConfig


class EEFeatures:
    def __init__(
        self,
        date_col: int,
        table_name: int,
        variable_satellites: zip(List[str]),
        static_satellites: zip(List[str]),
        bucket_name: str,
        path_to_private_key: str,
        service_account: str,
    ):

        self.date_col = date_col
        self.table_name = table_name
        self.variable_satellites = variable_satellites
        self.static_satellites = static_satellites
        self.bucket_name = bucket_name
        self.path_to_private_key = path_to_private_key
        self.service_account = service_account

    @classmethod
    def from_dataclass_config(cls, config: EEConfig) -> "EEFeatures":
        return cls(
            date_col=config.DATE_COL,
            table_name=config.TABLE_NAME,
            variable_satellites=config.VARIABLE_SATELLITES,
            static_satellites=config.STATIC_SATELLITES,
            bucket_name=config.BUCKET_NAME,
            path_to_private_key=config.PATH_TO_PRIVATE_KEY,
            service_account=config.SERVICE_ACCOUNT,
        )

    def execute(self, df, save_images):
        ee.Authenticate()
        # end_date, start_date = self._generate_timerange()
        satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_location)(lon, lat, day, save_images)
                for lon, lat, day in zip(df.x, df.y, df.day)
            ),
        ).reset_index(drop=True)
        features_df = self.generate_features(satellite_df)

        return features_df

    def execute_for_location(self, lon, lat, day, save_images):
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
        lon:
            the longitude of a  sensor location
        lat:
            the latitude of a sensor location
        datetime:
            the date the sensor reading was taken
        """
        ee.Initialize()

        df_list = []

        day_of_interest = ee.Date(day)
        centroid_point = ee.Geometry.Point(lon, lat)
        for (
            collection,
            image_bands,
            period,
            resolution,
        ) in self.variable_satellites:
            image_collection = self.execute_for_collection(
                collection,
                image_bands,
                save_images,
            )
            satellite_value = self._get_value_from_variable_collection(
                image_collection,
                day_of_interest,
                centroid_point,
                period,
                resolution,
            )
            try:
                ee_df = ee_array_to_df(satellite_value, image_bands)
                ee_df["x"] = lon
                ee_df["y"] = lat
                ee_df["date"] = day
                df_list.append(ee_df)
            except IndexError:
                pass
        for (
            collection,
            image_bands,
            resolution,
        ) in self.static_satellites:
            image_collection = self.execute_for_collection(
                collection,
                image_bands,
                save_images,
            )
            satellite_value = self._get_value_from_static_collection(
                image_collection,
                day_of_interest,
                centroid_point,
                resolution,
            )
            try:
                ee_df = ee_array_to_df(satellite_value, image_bands)
                ee_df["x"] = lon
                ee_df["y"] = lat
                ee_df["date"] = day
                df_list.append(ee_df)
            except IndexError:
                pass
        return pd.concat(df_list)

    def execute_for_collection(
        self,
        collection,
        image_bands,
        save_images,
    ):
        """
        Input
        ----
        Takes the name of a satellite image collection and the bands
        (fields) to extract data for, and for a given date range.

        Arguments:
        ----
        collection:
            A str of satellite to query
        image_bands:
            A list of bands captured for each satellite
        save_images:
            a boolean flag whether to write satellite data to google storage
        """
        ee.Initialize()

        # logging.info(
        #     "please sigup to Google Earth Engine here:"
        #     " https://signup.earthengine.google.com/"
        # )
        # if bucket.blob(f"{collection}_{s_datetime}_{e_datetime}"):
        try:
            logging.info(f"Downloading: {collection}")

            image_collection = ee.ImageCollection(collection).select(
                image_bands
            )

            if save_images is True:
                down_args = {
                    "image": image_collection,
                    "bucket": self.bucket_name,
                    "description": f"{collection}",
                    "scale": 30,
                }

                task = batch.Export.image.toCloudStorage(**down_args)
                task.start()
            else:
                return image_collection
        except (EEException, HttpError):
            logging.warning(
                f"""Image collection {image_collection.getInfo()}
                does not match any existing location."""
            )
            pass

    def generate_features(self, satellite_df):
        features_df = (
            satellite_df.drop(
                labels=["time", "datetime", "longitude", "latitude"], axis=1
            )
            .groupby(
                [
                    "date",
                    "x",
                    "y",
                ],
                as_index=False,
            )
            .agg(["mean"])
            .reset_index()
        )
        features_df.columns = list(map("".join, features_df.columns.values))
        return features_df

    def _generate_timerange(self) -> Tuple[str]:
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
        end_date = str(get_data(end_date_query)["datetime"][0])
        start_date = str(get_data(start_date_query)["datetime"][0])
        return end_date, start_date

    def _get_value_from_variable_collection(
        self,
        image_collection,
        day_of_interest,
        centroid_point,
        period,
        resolution,
    ):
        try:
            filtered_image_collection = image_collection.filterDate(
                day_of_interest, day_of_interest.advance(period, "day")
            )
            info = filtered_image_collection.getRegion(
                centroid_point, resolution
            ).getInfo()
            return info
        except (EEException, HttpError):
            # logging.warning(
            #     f"""Centroid location and date does not
            #     match any existing ee.Image."""
            # )
            pass

    def _get_value_from_static_collection(
        self, image_collection, day_of_interest, centroid_point, resolution
    ):
        try:
            filtered_image_collection = image_collection.filterDate(
                "2019-01-01", day_of_interest
            )
            info = filtered_image_collection.getRegion(
                centroid_point, resolution
            ).getInfo()

            return info
        except (EEException, HttpError):
            logging.warning(
                "Centroid location and date does not match any existing"
                " ee.Image."
            )
            pass

import datetime
import logging
from typing import List, Tuple

import ee
import pandas as pd
from ee.ee_exception import EEException
from geetools import batch
from googleapiclient.errors import HttpError
from haversine import haversine
from joblib import Parallel, delayed
from src.utils.utils import ee_array_to_df, get_data

from config.model_settings import EEConfig


class EEFeatures:
    def __init__(
        self,
        date_col: int,
        table_name: int,
        all_satellites: zip(List[str]),
        bucket_name: str,
        path_to_private_key: str,
        service_account: str,
        lookback_n: int,
    ):

        self.date_col = date_col
        self.table_name = table_name
        self.all_satellites = all_satellites
        self.bucket_name = bucket_name
        self.path_to_private_key = path_to_private_key
        self.service_account = service_account
        self.lookback_n = lookback_n

    @classmethod
    def from_dataclass_config(cls, config: EEConfig) -> "EEFeatures":
        return cls(
            date_col=config.DATE_COL,
            table_name=config.TABLE_NAME,
            all_satellites=config.ALL_SATELLITES,
            bucket_name=config.BUCKET_NAME,
            path_to_private_key=config.PATH_TO_PRIVATE_KEY,
            service_account=config.SERVICE_ACCOUNT,
            lookback_n=config.LOOKBACK_N,
        )

    def execute(self, df, save_images):
        ee.Authenticate()
        # end_date, start_date = self._generate_timerange()
        satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_location)(lon, lat, day, save_images)
                for lon, lat, day in zip(df.x, df.y, df.timestamp_utc)
            ),
        ).reset_index(drop=True)
        print(satellite_df)
        features_df = self.generate_features(satellite_df)

        return features_df

    def execute_for_location(self, lon, lat, date_utc, save_images):
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

        for (
            collection,
            image_bands,
            period,
            resolution,
        ) in self.all_satellites:
            image_collection = self.execute_for_collection(
                collection,
                image_bands,
                save_images,
            )
            ee_df = self.get_satellite_data(
                image_collection,
                image_bands,
                date_utc,
                lon,
                lat,
                period,
                resolution,
            )
            try:
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
                    "sensor_datetime",
                    "sensor_longitude",
                    "sensor_latitude",
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

    def get_satellite_data(
        self,
        image_collection,
        image_bands,
        day,
        lon,
        lat,
        period,
        resolution,
    ):
        """This function builds an algorithm to compute
        thr representative satellite value for a sensor location."""

        try:
            logging.info("Getting Most recent image info")
            ee_df = self.get_most_recent_satellite_data(
                image_collection,
                image_bands,
                lon,
                lat,
                resolution,
                day,
                period,
            )

            return ee_df
        except (EEException, HttpError):
            logging.info(
                "Finding ee.ImageCollection between"
                f" {day} and"
                f" {self.lookback_n * period} days"
            )
            try:
                ee_df = self.get_satellite_data_within_lookback(
                    image_collection,
                    image_bands,
                    lon,
                    lat,
                    resolution,
                    day,
                    period,
                )
                return ee_df
            except (EEException, HttpError):
                try:
                    ee_df = self.get_any_recent_satellite_data(
                        image_collection,
                        image_bands,
                        lon,
                        lat,
                        resolution,
                        day,
                    )
                    return ee_df
                except (EEException, HttpError):
                    logging.warn(f"No image available for {lon}, {lat}")
                    pass

    def get_most_recent_satellite_data(
        self,
        image_collection,
        image_bands,
        lon,
        lat,
        resolution,
        date_utc,
        period,
    ):
        """
        This function takes in an image collection
        and a set of spatial and temporal parameters
        to calculate the weighted temporal average
        value for each satellite query given a time period.

        Arguments
        -------
        """
        centroid_point = ee.Geometry.Point(lon, lat)
        day_of_interest = ee.Date(date_utc)

        filtered_image_collection = image_collection.filterDate(
            day_of_interest.advance(-period, "days"), day_of_interest
        )
        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, date_utc, lon, lat
        )

    def get_satellite_data_within_lookback(
        self,
        image_collection,
        image_bands,
        lon,
        lat,
        resolution,
        date_utc,
        period,
    ):

        centroid_point = ee.Geometry.Point(lon, lat)
        day_of_interest = ee.Date(date_utc)

        filtered_image_collection = image_collection.filterDate(
            day_of_interest.advance(-(self.lookback_n * period), "days"),
            day_of_interest,
        )
        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()
        return self._create_satellite_dataframe(
            info, image_bands, date_utc, lon, lat
        )

    def get_any_recent_satellite_data(
        self,
        image_collection,
        image_bands,
        lon,
        lat,
        resolution,
        date_utc,
    ):
        centroid_point = ee.Geometry.Point(lon, lat)
        day_of_interest = ee.Date(date_utc)
        start_date = ee.Date(
            "2015-01-01",
        )
        filtered_image_collection = image_collection.filterDate(
            start_date,
            day_of_interest,
        )
        filtered_image_collection = image_collection.limit(10)
        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()
        return self._create_satellite_dataframe(
            info, image_bands, date_utc, lon, lat
        )

    def _create_satellite_dataframe(
        self, info, image_bands, date_utc, lon, lat
    ):
        """Creates a dataframe from returned satellite information and"""
        """Builds required fields for weighted average calculation"""
        ee_df = ee_array_to_df(info, image_bands)
        ee_df = self._calculate_temporal_weighted_average(date_utc, ee_df)
        ee_df = self._calculate_spatial_weighted_average(ee_df, lon, lat)
        return ee_df

    def _calculate_temporal_weighted_average(self, date_utc, ee_df):
        """Calculate the difference between the sensor timestamp and the"""
        """satellite timestamp for all returned values"""
        ee_df["sensor_datetime"] = datetime.datetime.strptime(
            date_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        ee_df["sensor_timestamp"] = ee_df.sensor_datetime.astype("int64")
        ee_df["satellite_timestamp"] = ee_df.datetime.astype("int64")

        ee_df["timestamp_diff"] = (
            ee_df["sensor_timestamp"] - ee_df["satellite_timestamp"]
        )
        return ee_df

    def _calculate_spatial_weighted_average(self, ee_df, lon, lat):
        """Calculate the spatially-weighted distance"""
        ee_df["sensor_longitude"] = lon
        ee_df["sensor_latitude"] = lat
        try:
            ee_df["distance"] = ee_df.apply(
                lambda row: haversine(
                    (row["sensor_longitude"], row["sensor_latitude"]),
                    (row["longitude"], row["latitude"]),
                    unit="m",
                ),
                axis=1,
            )
            return ee_df

        except ValueError:
            pass

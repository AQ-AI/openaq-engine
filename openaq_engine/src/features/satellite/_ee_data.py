import datetime
import logging
from collections import Counter
from typing import List, Tuple

import ee
import numpy as np
import pandas as pd
from ee.ee_exception import EEException
from geetools import batch
from googleapiclient.errors import HttpError
from haversine import haversine
from joblib import Parallel, delayed
from setup_environment import get_dbengine
from sklearn.preprocessing import MinMaxScaler

from src.utils.utils import ee_array_to_df, get_data, write_to_db

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

    def execute(self, x, y, timestamp_utc, save_images):
        credentials = ee.ServiceAccountCredentials(
            self.service_account,
            self.path_to_private_key,
        )
        ee.Initialize(credentials)

        satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_satellite)(
                    x, y, timestamp_utc, satellite, save_images
                )
                for satellite in self.all_satellites
            ),
        ).reset_index(drop=True)

        # features_df = self.generate_features(satellite_df)

        return satellite_df

    def execute_for_satellite(
        self, x, y, timestamp_utc, satellite, save_images
    ):
        """
        Input
        ----
        Takes the name of a satellite image collection and the bands
        (fields) to extract data for, and for a given date range writes the
        extracted satellite `.tiff` files to a google file structure.

        Arguments:
        ----
        x:
            the longitude of a sensor location
        y:
            the latitude of a sensor location
        timestamp_utc:
            the timestamp of the sensor reading
        satellite:
            A tuple containing satellite configuration
        """
        collection, image_bands, period, resolution = satellite

        image_collection = self.execute_for_collection(
            collection,
            image_bands,
            save_images,
        )

        ee_df = self.get_satellite_data(
            image_collection,
            image_bands,
            x,
            y,
            timestamp_utc,
            period,
            resolution,
        )

        engine = get_dbengine()
        if not ee_df.empty:
            write_to_db(
                ee_df,
                engine,
                f"{collection.replace('/', '_')}_local_MN",
                "public",
                "append",
            )

        return ee_df

    def execute_for_collection(self, collection, image_bands, save_images):
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
        try:
            logging.info(f"Downloading: {collection}")

            available_bands = (
                ee.ImageCollection(collection).first().bandNames().getInfo()
            )
            logging.info(f"Available bands in {collection}: {available_bands}")

            if not all(band in available_bands for band in image_bands):
                raise ValueError(
                    f"Bands {image_bands} are not available in the collection {collection}"
                )

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
                f"""Image collection {collection.getInfo()}
                does not match any existing location."""
            )

    def bands_available(self, image_collection, image_bands):
        """Check if the specified bands are available in the image collection."""
        try:
            first_image = image_collection.first().select(image_bands)
            first_image.getInfo()
            return True
        except Exception as e:
            logging.error(
                f"Specified bands {image_bands} are not available: {e}"
            )
            return False

    def _create_satellite_dataframe(
        self, info, image_bands, timestamp_utc, lon, lat
    ):
        """Create a DataFrame from the satellite data."""
        ee_df = ee_array_to_df(info, image_bands)
        # ee_df = self._calculate_temporal_weighted_average(timestamp_utc, ee_df)
        ee_df = self._calculate_spatial_weighted_average(ee_df, lon, lat)
        ee_df["timestamp_utc"] = timestamp_utc
        return ee_df

    def generate_features(self, satellite_df):
        groupby_cols = [
            "sensor_datetime",
            "sensor_longitude",
            "sensor_latitude",
            "location_id",
        ]

        weights = ["timestamp_diff", "distance"]
        cols_to_remove = [
            "longitude",
            "latitude",
            "cohort",
            "time",
            "datetime",
            "sensor_timestamp",
            "satellite_timestamp",
        ]
        satellite_df = satellite_df.drop(cols_to_remove, axis=1)
        avg_cols = [
            i
            for i in list(satellite_df.columns)
            if i
            not in list((Counter(groupby_cols) + Counter(weights)).elements())
        ]

        features_df = self._weighted_mean_by_lambda(
            satellite_df, avg_cols, weights, groupby_cols
        )
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
        lon,
        lat,
        date_utc,
        period,
        resolution,
    ):
        """This function builds an algorithm to compute the representative satellite value for a sensor location."""
        try:
            ee_df = self.get_satellite_data_within_hour(
                image_collection,
                image_bands,
                date_utc,
                lon,
                lat,
                resolution,
                period,
            )
            if not ee_df.empty:
                logging.info("Getting Most recent image info")
                return ee_df
            else:
                pass
        except Exception as e:
            logging.error(f"Error retrieving satellite data: {e}")

        # try:
        #     ee_df = self.get_satellite_data_within_lookback(
        #         image_collection, image_bands, location_id, lon, lat, cohort, resolution, date_utc, period)
        #     if not ee_df.empty:
        #         logging.info("Finding ee.ImageCollection between"
        #                      f" {date_utc} and"
        #                      f" {self.config.LOOKBACK_N * period} days")
        #         print("Within lookback image: ", ee_df)
        #         return ee_df
        #     else:
        #         pass
        # except Exception as e:
        #     logging.error(f"Error retrieving satellite data within lookback: {e}")

        # try:
        #     ee_df = self.get_any_recent_satellite_data(
        #         image_collection, image_bands, location_id, lon, lat, cohort, resolution, date_utc)
        #     if not ee_df.empty:
        #         logging.info("Finding ee.ImageCollection after 2015")
        #         print("any image: ", ee_df)
        #         return ee_df
        #     else:
        #         pass
        # except Exception as e:
        #     logging.error(f"Error retrieving any recent satellite data: {e}")

        logging.warning(f"No image available for {lon}, {lat}, {date_utc}")
        return pd.DataFrame()

    def get_most_recent_satellite_data(
        self,
        image_collection,
        image_bands,
        location_id,
        lon,
        lat,
        cohort,
        resolution,
        date_utc,
        period,
    ):
        """Fetch the most recent satellite data for the given parameters."""
        centroid_point = ee.Geometry.Point(lon, lat)
        day_of_interest = ee.Date(date_utc)
        filtered_image_collection = image_collection.filterDate(
            day_of_interest.advance(-period, "days"), day_of_interest
        )

        # Check if the bands are available in the filtered collection
        if not self.bands_available(filtered_image_collection, image_bands):
            raise ValueError("No bands in collection")

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, location_id, date_utc, lon, lat
        )

    def get_satellite_data_within_hour(
        self,
        image_collection,
        image_bands,
        date_utc,
        lon,
        lat,
        resolution,
        period,
    ):
        """
        This function takes in an image collection and a set of spatial and temporal parameters
        to calculate the satellite value for a sensor location within the hour of the sensor reading.
        """
        centroid_point = ee.Geometry.Point(lon, lat)

        if isinstance(date_utc, pd.Timestamp):
            date_utc = date_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        sensor_datetime = datetime.datetime.strptime(
            date_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        start_of_hour = sensor_datetime.replace(
            minute=0, second=0, microsecond=0
        )
        end_of_hour = start_of_hour + datetime.timedelta(hours=1)

        filtered_image_collection = image_collection.filterDate(
            ee.Date(start_of_hour.isoformat()),
            ee.Date(end_of_hour.isoformat()),
        )
        # Check if the bands are available in the filtered collection
        if not self.bands_available(filtered_image_collection, image_bands):
            raise ValueError("No bands in collection")

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, date_utc, lon, lat
        )

    def _calculate_temporal_weighted_average(self, date_utc, ee_df):
        """Calculate the difference between the sensor timestamp and the
        satellite timestamp for all returned values"""
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
        ee_df["distance"] = ee_df.apply(self.calculate_distance, axis=1)
        return ee_df

    def calculate_distance(self, row):
        try:
            return haversine(
                (row["sensor_latitude"], row["sensor_longitude"]),
                (row["latitude"], row["longitude"]),
                unit="m",
            )
        except Exception as e:
            print(f"Error processing row {row}: {e}")
            return None  # or appropriate error value

    def _weighted_mean_by_lambda(
        self, df, avg_cols, weight_cols, groupby_cols
    ):
        def _scale_weight_cols(df, weight_cols):
            """This takes in a DataFrame and columns used to construct
            a weight column using the MinMaxScalar() function"""
            scaler = MinMaxScaler()
            df[weight_cols] = scaler.fit_transform(df[weight_cols])
            df["weight"] = df.loc[:, weight_cols].prod(axis=1)
            return df

        def _weighted_means_by_column_ignoring_NaNs(x, cols, w="weights"):
            """This takes a DataFrame and averages each data column (cols),
            weighting observations by column w, but ignoring individual NaN
            observations within each column.
            """
            try:
                return pd.Series(
                    [
                        np.nan
                        if x.dropna(subset=[c]).empty
                        else np.average(
                            x.dropna(subset=[c])[c],
                            weights=x.dropna(subset=[c])[w],
                        )
                        for c in cols
                    ],
                    cols,
                )
            except ZeroDivisionError:
                pd.Series(
                    [
                        np.nan
                        if x.dropna(subset=[c]).empty
                        else np.average(
                            x.dropna(subset=[c])[c],
                        )
                        for c in cols
                    ],
                    cols,
                )

        df = _scale_weight_cols(df, weight_cols)

        return (
            df.groupby(groupby_cols)
            .apply(_weighted_means_by_column_ignoring_NaNs, avg_cols, "weight")
            .reset_index()
        )

    def get_satellite_data_within_lookback(
        self,
        image_collection,
        image_bands,
        location_id,
        lon,
        lat,
        cohort,
        resolution,
        date_utc,
        period,
    ):
        """
        This function takes in an image collection
        and a set of spatial and temporal parameters
        to calculate the weighted temporal average
        value for each satellite within a lookback.
        """

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
            info, image_bands, date_utc, lon, lat, cohort
        )

    def get_any_recent_satellite_data(
        self,
        image_collection,
        image_bands,
        location_id,
        lon,
        lat,
        cohort,
        resolution,
        date_utc,
    ):
        """This function collects all satellite imagery from
        between the specified date and the first date for a specific
        geolocation with no time windor specified"""
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
            info,
            image_bands,
            date_utc,
            lon,
            lat,
        )

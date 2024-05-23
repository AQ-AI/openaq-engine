import datetime
import logging
from collections import Counter
from typing import Tuple

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
        date_col: str,
        satellite_config: dict,
        bucket_name: str,
        path_to_private_key: str,
        service_account: str,
        lookback_n: int,
    ):
        self.date_col = date_col
        self.satellite_config = satellite_config
        self.bucket_name = bucket_name
        self.path_to_private_key = path_to_private_key
        self.service_account = service_account
        self.lookback_n = lookback_n

    @classmethod
    def from_dataclass_config(cls, config: EEConfig) -> "EEFeatures":
        return cls(
            date_col=config.DATE_COL,
            satellite_config=config.SATELLITE_CONFIG,
            bucket_name=config.BUCKET_NAME,
            path_to_private_key=config.PATH_TO_PRIVATE_KEY,
            service_account=config.SERVICE_ACCOUNT,
            lookback_n=config.LOOKBACK_N,
        )

    def execute(self, x, y, table_name, save_images):
        credentials = ee.ServiceAccountCredentials(
            self.service_account,
            self.path_to_private_key,
        )
        ee.Initialize(credentials)

        results = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.query_satellite_for_time_range)(
                satellite, config, x, y, table_name, save_images
            )
            for satellite, config in self.satellite_config.items()
        )

        # Flatten the list of DataFrames
        satellite_dfs = [
            result
            for sublist in results
            for result in sublist
            if result is not None
        ]

        if satellite_dfs:
            satellite_df = pd.concat(satellite_dfs).reset_index(drop=True)
            return satellite_df
        else:
            return pd.DataFrame()

    def query_satellite_for_time_range(
        self, satellite, config, x, y, table_name, save_images
    ):
        satellite_dfs = []
        for time_range in config["time_ranges"]:
            start_time = datetime.datetime.strptime(
                time_range[0], "%H:%M:%S"
            ).time()
            end_time = datetime.datetime.strptime(
                time_range[1], "%H:%M:%S"
            ).time()

            if config["frequency"] == "daily":
                delta = datetime.timedelta(days=1)
            elif config["frequency"] == "monthly":
                delta = pd.DateOffset(months=1)
            elif config["frequency"] == "annual":
                delta = pd.DateOffset(years=1)
            else:
                delta = datetime.timedelta(days=1)

            start_date_query = f"""SELECT MIN("timestamp_utc") AS start_date FROM "{table_name}";"""
            end_date_query = f"""SELECT MAX("timestamp_utc") AS end_date FROM "{table_name}";"""
            start_date = pd.to_datetime(
                get_data(start_date_query)["start_date"].iloc[0]
            )
            end_date = pd.to_datetime(
                get_data(end_date_query)["end_date"].iloc[0]
            )

            current_date = start_date

            while current_date <= end_date:
                start_datetime = datetime.datetime.combine(
                    current_date, start_time
                )
                end_datetime = datetime.datetime.combine(
                    current_date, end_time
                )

                logging.info(
                    f"Querying {satellite} for location ({x}, {y}) between {start_datetime} and {end_datetime}"
                )

                result = self.query_satellite(
                    satellite,
                    config["bands"],
                    start_datetime,
                    end_datetime,
                    x,
                    y,
                    config["resolution"],
                    save_images,
                )

                if result is not None:
                    satellite_dfs.append(result)

                current_date += delta

        return satellite_dfs

    def query_satellite(
        self,
        satellite,
        bands,
        start_datetime,
        end_datetime,
        x,
        y,
        resolution,
        save_images,
    ):
        image_collection = self.execute_for_collection(
            satellite, bands, save_images
        )
        ee_df = self.get_satellite_data(
            image_collection,
            bands,
            x,
            y,
            start_datetime,
            end_datetime,
            resolution,
        )

        engine = get_dbengine()
        if not ee_df.empty:
            write_to_db(
                ee_df,
                engine,
                f"{satellite.replace('/', '_')}_local_MN",
                "public",
                "append",
            )

        return ee_df

    def execute_for_collection(self, collection, image_bands, save_images):
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

    def _generate_timerange(self, table_name) -> Tuple[str]:
        start_date_query = f"""SELECT MIN("{self.date_col}") AS datetime FROM "{table_name}";"""
        end_date_query = f"""SELECT MAX("{self.date_col}") AS datetime FROM "{table_name}";"""
        end_date = str(get_data(end_date_query)["datetime"][0])
        start_date = str(get_data(start_date_query)["datetime"][0])
        return end_date, start_date

    def get_satellite_data(
        self,
        image_collection,
        image_bands,
        lon,
        lat,
        start_datetime,
        end_datetime,
        resolution,
    ):
        """This function builds an algorithm to compute the representative satellite value for a sensor location."""
        try:
            ee_df = self.get_satellite_data_within_hour(
                image_collection,
                image_bands,
                start_datetime,
                end_datetime,
                lon,
                lat,
                resolution,
            )
            if not ee_df.empty:
                logging.info("Getting Most recent image info")
                return ee_df
            else:
                pass
        except Exception as e:
            logging.error(f"Error retrieving satellite data: {e}")

        logging.warning(
            f"No image available for {lon}, {lat}, {start_datetime}"
        )
        return pd.DataFrame()

    def get_satellite_data_within_hour(
        self,
        image_collection,
        image_bands,
        start_datetime,
        end_datetime,
        lon,
        lat,
        resolution,
    ):
        """
        This function takes in an image collection and a set of spatial and temporal parameters
        to calculate the satellite value for a sensor location within the hour of the sensor reading.
        """
        centroid_point = ee.Geometry.Point(lon, lat)

        filtered_image_collection = image_collection.filterDate(
            ee.Date(start_datetime.isoformat()),
            ee.Date(end_datetime.isoformat()),
        )
        # Check if the bands are available in the filtered collection
        if not self.bands_available(filtered_image_collection, image_bands):
            raise ValueError("No bands in collection")

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, start_datetime, lon, lat
        )

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

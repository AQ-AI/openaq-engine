import datetime
import logging
import os
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

from config.model_settings import EEConfig, MatrixGeneratorConfig


class EEFeatures:
    """
    Class to handle the extraction and processing of Earth Engine (EE) features.

    Parameters
    ----------
    date_col : int
        The column index or name representing the date.
    table_name : int
        The name of the table from which data is being extracted.
    all_satellites : List[Tuple[str, List[str], int, int]]
        A list of tuples containing satellite collection names, image bands, period, and resolution.
    bucket_name : str
        The name of the Google Cloud Storage bucket where images will be saved.
    path_to_private_key : str
        The path to the private key for Google service account authentication.
    service_account : str
        The email of the Google service account.
    lookback_n : int
        The number of lookback periods for temporal data retrieval.
    """

    def __init__(
        self,
        satellite_config: dict,
        date_col: str,
        bucket_name: str,
        lookback_n: int,
    ):
        self.satellite_config = satellite_config
        self.date_col = date_col
        self.bucket_name = bucket_name
        self.lookback_n = lookback_n
        # Retrieve service account email and path to private key from environment variables
        self.service_account = os.getenv("SERVICE_ACCOUNT_EMAIL")
        self.path_to_private_key = os.getenv("EARTHENGINE_CREDENTIALS")

        if not self.service_account or not self.path_to_private_key:
            raise ValueError(
                "Environment variables for service account credentials are not set."
            )

    @classmethod
    def from_dataclass_config(cls, config: EEConfig) -> "EEFeatures":
        """
        Create an instance of EEFeatures from a configuration dataclass.

        Parameters
        ----------
        config : EEConfig
            The configuration dataclass.

        Returns
        -------
        EEFeatures
            An instance of EEFeatures.
        """
        return cls(
            satellite_config=MatrixGeneratorConfig.SATELLITE_CONFIG,
            date_col=config.DATE_COL,
            bucket_name=config.BUCKET_NAME,
            lookback_n=config.LOOKBACK_N,
        )

    def execute(self, x, y, table_name, save_images, use_parallel=True):
        """
        Create an instance of EEFeatures from a configuration dataclass.

        Parameters
        ----------
        config : EEConfig
            The configuration dataclass.

        Returns
        -------
        EEFeatures
            An instance of EEFeatures.
        """
        credentials = ee.ServiceAccountCredentials(
            self.service_account,
            self.path_to_private_key,
        )
        ee.Initialize(credentials)

        if use_parallel:
            results = Parallel(
                n_jobs=-1, backend="multiprocessing", verbose=5
            )(
                delayed(self.query_satellite_for_time_range)(
                    satellite, config, x, y, table_name, save_images
                )
                for satellite, config in self.satellite_config.items()
            )
        else:
            results = [
                self.query_satellite_for_time_range(
                    satellite, config, x, y, table_name, save_images
                )
                for satellite, config in self.satellite_config.items()
            ]

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
                    table_name,
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
        table_name,
        bands,
        start_datetime,
        end_datetime,
        x,
        y,
        resolution,
        save_images,
    ):
        """
        Retrieve satellite image collection and optionally save it to Google Cloud Storage.

        Parameters
        ----------
        collection : str
            The name of the satellite image collection.
        image_bands : List[str]
            The list of bands to extract from the satellite images.
        save_images : bool
            Whether to save the satellite images to Google Cloud Storage.

        Returns
        -------
        ee.ImageCollection
            The Earth Engine image collection.
        """
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

        engine = get_dbengine(
            os.getenv("PGDATABASE"),
            os.getenv("PGHOST"),
            os.getenv("PGPORT"),
            os.getenv("PGUSER"),
            os.getenv("PGPASSWORD"),
        )
        if not ee_df.empty:
            write_to_db(
                ee_df,
                engine,
                f"{satellite.replace('/', '_')}_{table_name}",
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
        """
        Get satellite data for a specific location and time.

        Parameters
        ----------
        image_collection : ee.ImageCollection
            The Earth Engine image collection.
        image_bands : List[str]
            The list of bands to extract from the satellite images.
        location_id : str
            The ID of the location.
        date_utc : str
            The UTC date and time of the observation.
        lon : float
            The longitude of the location.
        lat : float
            The latitude of the location.
        period : int
            The time period to look back for satellite data.
        resolution : int
            The spatial resolution of the satellite images.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the satellite data for the location.
        """
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
        Get satellite data for a location within the hour of the sensor reading.

        Parameters
        ----------
        image_collection : ee.ImageCollection
            The Earth Engine image collection.
        image_bands : List[str]
            The list of bands to extract from the satellite images.
        location_id : str
            The ID of the location.
        lon : float
            The longitude of the location.
        lat : float
            The latitude of the location.
        resolution : int
            The spatial resolution of the satellite images.
        date_utc : str
            The UTC date and time of the observation.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the satellite data for the location within the hour.
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
                        (
                            np.nan
                            if x.dropna(subset=[c]).empty
                            else np.average(
                                x.dropna(subset=[c])[c],
                                weights=x.dropna(subset=[c])[w],
                            )
                        )
                        for c in cols
                    ],
                    cols,
                )
            except ZeroDivisionError:
                pd.Series(
                    [
                        (
                            np.nan
                            if x.dropna(subset=[c]).empty
                            else np.average(
                                x.dropna(subset=[c])[c],
                            )
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

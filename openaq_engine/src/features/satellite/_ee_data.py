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
from sklearn.preprocessing import MinMaxScaler

from config.model_settings import EEConfig
from openaq_engine.src.utils.utils import ee_array_to_df, get_data


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
        date_col: int,
        table_name: int,
        all_satellites: List[Tuple[str, List[str], int, int]],
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
            date_col=config.DATE_COL,
            table_name=config.TABLE_NAME,
            all_satellites=config.ALL_SATELLITES,
            bucket_name=config.BUCKET_NAME,
            path_to_private_key=config.PATH_TO_PRIVATE_KEY,
            service_account=config.SERVICE_ACCOUNT,
            lookback_n=config.LOOKBACK_N,
        )

    def execute(self, df: pd.DataFrame, save_images: bool) -> pd.DataFrame:
        """
        Execute the feature extraction process for all locations.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the location data.
        save_images : bool
            Whether to save the satellite images to Google Cloud Storage.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the extracted satellite features.
        """
        ee.Authenticate()
        satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_location)(
                    location_id, lon, lat, day, save_images
                )
                for location_id, lon, lat, day in zip(
                    df.locationId, df.x, df.y, df.timestamp_utc
                )
            ),
        ).reset_index(drop=True)
        features_df = self.generate_features(satellite_df)

        return features_df

    def execute_for_location(
        self,
        location_id: str,
        lon: float,
        lat: float,
        date_utc: str,
        save_images: bool,
    ) -> pd.DataFrame:
        """
        Execute the feature extraction process for a specific location.

        Parameters
        ----------
        location_id : str
            The ID of the location.
        lon : float
            The longitude of the location.
        lat : float
            The latitude of the location.
        date_utc : str
            The UTC date and time of the observation.
        save_images : bool
            Whether to save the satellite images to Google Cloud Storage.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the extracted satellite features for the location.
        """
        ee.Initialize()

        df_list = []

        for collection, image_bands, period, resolution in self.all_satellites:
            image_collection = self.execute_for_collection(
                collection,
                image_bands,
                save_images,
            )
            ee_df = self.get_satellite_data(
                image_collection,
                image_bands,
                location_id,
                date_utc,
                lon,
                lat,
                period,
                resolution,
            )
            if not ee_df.empty:
                df_list.append(ee_df)
        try:
            return pd.concat(df_list).reset_index(drop=True)
        except ValueError:
            return pd.DataFrame()

    def execute_for_collection(
        self,
        collection: str,
        image_bands: List[str],
        save_images: bool,
    ) -> ee.ImageCollection:
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
        ee.Initialize()

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
                f"Image collection {image_collection.getInfo()} does not match any existing location."
            )
            pass

    def generate_features(self, satellite_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate features from the satellite data.

        Parameters
        ----------
        satellite_df : pd.DataFrame
            The DataFrame containing satellite data.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the generated features.
        """
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

    def _generate_timerange(self) -> Tuple[str, str]:
        """
        Generate the start and end date for satellite data extraction.

        Returns
        -------
        Tuple[str, str]
            A tuple containing the start and end dates as strings.
        """
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
        image_collection: ee.ImageCollection,
        image_bands: List[str],
        location_id: str,
        date_utc: str,
        lon: float,
        lat: float,
        period: int,
        resolution: int,
    ) -> pd.DataFrame:
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
                location_id,
                lon,
                lat,
                resolution,
                date_utc,
            )
            if not ee_df.empty:
                logging.info(
                    "Getting satellite data within the hour of interest"
                )
                return ee_df
            else:
                logging.warning(
                    f"No matching satellite data within the hour for {lon}, {lat} at {date_utc}"
                )
                return pd.DataFrame()
        except (EEException, HttpError) as e:
            logging.error(f"Error retrieving satellite data: {e}")
            return pd.DataFrame()

    def get_satellite_data_within_hour(
        self,
        image_collection: ee.ImageCollection,
        image_bands: List[str],
        location_id: str,
        lon: float,
        lat: float,
        resolution: int,
        date_utc: str,
    ) -> pd.DataFrame:
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

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, location_id, date_utc, lon, lat
        )

    def get_most_recent_satellite_data(
        self,
        image_collection: ee.ImageCollection,
        image_bands: List[str],
        location_id: str,
        lon: float,
        lat: float,
        resolution: int,
        date_utc: str,
        period: int,
    ) -> pd.DataFrame:
        """
        Get the most recent satellite data for a location within a specified time period.

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
        period : int
            The time period to look back for satellite data.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the most recent satellite data for the location.
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
            info, image_bands, location_id, date_utc, lon, lat
        )

    def get_satellite_data_within_lookback(
        self,
        image_collection: ee.ImageCollection,
        image_bands: List[str],
        location_id: str,
        lon: float,
        lat: float,
        resolution: int,
        date_utc: str,
        period: int,
    ) -> pd.DataFrame:
        """
        Get satellite data within a specified lookback period.

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
        period : int
            The time period to look back for satellite data.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the satellite data for the location within the lookback period.
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
            info, image_bands, location_id, date_utc, lon, lat
        )

    def get_any_recent_satellite_data(
        self,
        image_collection: ee.ImageCollection,
        image_bands: List[str],
        location_id: str,
        lon: float,
        lat: float,
        resolution: int,
        date_utc: str,
    ) -> pd.DataFrame:
        """
        Get any recent satellite data for a location from 2015-01-01 to the specified date.

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
            The DataFrame containing the recent satellite data for the location.
        """
        centroid_point = ee.Geometry.Point(lon, lat)
        day_of_interest = ee.Date(date_utc)
        start_date = ee.Date("2015-01-01")
        filtered_image_collection = image_collection.filterDate(
            start_date, day_of_interest
        )
        filtered_image_collection = image_collection.limit(10)
        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()
        return self._create_satellite_dataframe(
            info, image_bands, location_id, date_utc, lon, lat
        )

    def _create_satellite_dataframe(
        self,
        info: List,
        image_bands: List[str],
        location_id: str,
        date_utc: str,
        lon: float,
        lat: float,
    ) -> pd.DataFrame:
        """
        Create a DataFrame from satellite data and calculate temporal and spatial weighted averages.

        Parameters
        ----------
        info : List
            The list of information returned from Earth Engine.
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

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the satellite data with weighted averages.
        """
        ee_df = ee_array_to_df(info, image_bands)
        ee_df = self._calculate_temporal_weighted_average(date_utc, ee_df)
        ee_df = self._calculate_spatial_weighted_average(
            ee_df, lon, lat, location_id
        )
        return ee_df

    def _calculate_temporal_weighted_average(
        self, date_utc: str, ee_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate the temporal weighted average for the satellite data.

        Parameters
        ----------
        date_utc : str
            The UTC date and time of the observation.
        ee_df : pd.DataFrame
            The DataFrame containing the satellite data.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the temporal weighted average calculated.
        """
        ee_df["sensor_datetime"] = datetime.datetime.strptime(
            date_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        ee_df["sensor_timestamp"] = ee_df.sensor_datetime.astype("int64")
        ee_df["satellite_timestamp"] = ee_df.datetime.astype("int64")

        ee_df["timestamp_diff"] = (
            ee_df["sensor_timestamp"] - ee_df["satellite_timestamp"]
        )
        return ee_df

    def _calculate_spatial_weighted_average(
        self, ee_df: pd.DataFrame, lon: float, lat: float, location_id: str
    ) -> pd.DataFrame:
        """
        Calculate the spatially-weighted average for the satellite data.

        Parameters
        ----------
        ee_df : pd.DataFrame
            The DataFrame containing the satellite data.
        lon : float
            The longitude of the location.
        lat : float
            The latitude of the location.
        location_id : str
            The ID of the location.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the spatial weighted average calculated.
        """
        ee_df["sensor_longitude"] = lon
        ee_df["sensor_latitude"] = lat
        ee_df["location_id"] = location_id

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

    def _weighted_mean_by_lambda(
        self,
        df: pd.DataFrame,
        avg_cols: List[str],
        weight_cols: List[str],
        groupby_cols: List[str],
    ) -> pd.DataFrame:
        """
        Calculate the weighted mean for the satellite data by grouping and averaging.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the satellite data.
        avg_cols : List[str]
            The columns to average.
        weight_cols : List[str]
            The columns to use for weighting.
        groupby_cols : List[str]
            The columns to group by.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the weighted mean calculated.
        """

        def _scale_weight_cols(df, weight_cols):
            """Scale the weight columns using MinMaxScaler."""
            scaler = MinMaxScaler()
            df[weight_cols] = scaler.fit_transform(df[weight_cols])
            df["weight"] = df.loc[:, weight_cols].prod(axis=1)
            return df

        def _weighted_means_by_column_ignoring_NaNs(x, cols, w="weight"):
            """Calculate weighted mean for each column, ignoring NaNs."""
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

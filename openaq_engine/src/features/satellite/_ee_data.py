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

        print(features_df)


    def execute_for_location(
        self, location_id, lon, lat, date_utc, cohort, save_images
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
        lon:
            the longitude of a  sensor location
        lat:
            the latitude of a sensor location
        datetime:
            the date the sensor reading was taken
        """
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
            if not ee_df.empty:
                df_list.append(ee_df)
        try:
            return pd.concat(df_list).reset_index(drop=True)
        except ValueError:
            return pd.DataFrame()


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

            image_collection = ee.ImageCollection(collection).select(image_bands)

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
                    "x",
                    "y",
                ],
                as_index=False,
            )
            .agg(["mean"])
            .reset_index()
        )

        print(features_df)

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
        image_bands,
        location_id,
        date_utc,
        lon,
        lat,
        cohort,
        period,
        resolution,
    ):
        """
        This function builds an algorithm to compute
        the representative satellite value for a sensor location,
        considering only the exact location and time of flyover (within the hour).
        """
        try:
            ee_df = self.get_satellite_data_within_hour(
                image_collection,
                image_bands,
                location_id,
                lon,
                lat,
                cohort,
                resolution,
                date_utc,
            )
            if not ee_df.empty:
                logging.info("Getting satellite data within the hour of interest")
                print("Hourly image: ", ee_df)
                return ee_df
            else:
                logging.warn(f"No matching satellite data within the hour for {lon}, {lat} at {date_utc}")
                return pd.DataFrame()
        except (EEException, HttpError) as e:
            logging.error(f"Error retrieving satellite data: {e}")
            return pd.DataFrame()

    def get_satellite_data_within_hour(
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
        """
        This function takes in an image collection and a set of spatial and temporal parameters
        to calculate the satellite value for a sensor location within the hour of the sensor reading.
        """
        centroid_point = ee.Geometry.Point(lon, lat)
        sensor_datetime = datetime.datetime.strptime(date_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
        start_of_hour = sensor_datetime.replace(minute=0, second=0, microsecond=0)
        end_of_hour = start_of_hour + datetime.timedelta(hours=1)

        filtered_image_collection = image_collection.filterDate(
            ee.Date(start_of_hour.isoformat()), ee.Date(end_of_hour.isoformat())
        )

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, location_id, date_utc, lon, lat, cohort
        )


    def get_satellite_data_within_hour(
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
        """
        This function takes in an image collection and a set of spatial and temporal parameters
        to calculate the satellite value for a sensor location within the hour of the sensor reading.
        """
        centroid_point = ee.Geometry.Point(lon, lat)
        sensor_datetime = datetime.datetime.strptime(date_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
        start_of_hour = sensor_datetime.replace(minute=0, second=0, microsecond=0)
        end_of_hour = start_of_hour + datetime.timedelta(hours=1)

        filtered_image_collection = image_collection.filterDate(
            ee.Date(start_of_hour.isoformat()), ee.Date(end_of_hour.isoformat())
        )

        info = filtered_image_collection.getRegion(
            centroid_point, resolution
        ).getInfo()

        return self._create_satellite_dataframe(
            info, image_bands, location_id, date_utc, lon, lat, cohort
        )


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
        """
        This function takes in an image collection
        and a set of spatial and temporal parameters
        to calculate the weighted temporal average
        value for each satellite query given a time period.

        Arguments
        -------
        image_collection: str
            the string of an image collection
        image_bands: List[str]
            the list of image bands used (satellite model features)
        location_id: str
            the location_id of the sensor
        lon: float
            Longitude of sensor
        lat: float
            Latitude of sensor
        resolution: float
            resolution of image and used as satellite search radius
        date_utc: datetime
            datetime in utc of sensor reading
        period: int
            the number of days between satellite passovers
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
            info,
            image_bands,
            location_id,
            date_utc,
            lon,
            lat,
            cohort,
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
            info, image_bands, location_id, date_utc, lon, lat, cohort
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
            info, image_bands, location_id, date_utc, lon, lat, cohort
        )

    def _create_satellite_dataframe(
        self,
        info,
        image_bands,
        location_id,
        date_utc,
        lon,
        lat,
        cohort,
    ):
        """Creates a dataframe from returned satellite information and
        builds required fields for weighted average calculation"""
        ee_df = ee_array_to_df(info, image_bands)
        ee_df = self._calculate_temporal_weighted_average(date_utc, ee_df)
        ee_df = self._calculate_spatial_weighted_average(
            ee_df, lon, lat, location_id
        )
        ee_df["cohort"] = cohort
        ee_df["timestamp_utc"] = date_utc
        return ee_df

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

    def _calculate_spatial_weighted_average(
        self, ee_df, lon, lat, location_id
    ):
        """Calculate the spatially-weighted distance"""
        ee_df["sensor_longitude"] = lon
        ee_df["sensor_latitude"] = lat
        ee_df["location_id"] = location_id
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

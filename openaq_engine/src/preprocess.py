import json
import logging
import re
import warnings
from datetime import datetime, timezone
from typing import List

import pandas as pd
from shapely.errors import ShapelyDeprecationWarning
from shapely.geometry import Point

from config.model_settings import CohortBuilderConfig
from openaq_engine.src.preprocessing.filter import Filter


class Preprocess:
    """
    Class to preprocess raw input data by applying various filters and transformations.

    :param filter_pollutant: Whether to filter data based on specific pollutants, defaults to True.
    :type filter_pollutant: bool, optional
    :param filter_non_null_values: Whether to filter out rows with null values, defaults to True.
    :type filter_non_null_values: bool, optional
    :param filter_extreme_values: Whether to filter out extreme values, defaults to True.
    :type filter_extreme_values: bool, optional
    :param filter_no_coordinates: Whether to filter out rows with missing coordinates, defaults to True.
    :type filter_no_coordinates: bool, optional
    :param filter_countries: Whether to filter data based on countries, defaults to False.
    :type filter_countries: bool, optional
    :param filter_cities: Whether to filter data based on cities, defaults to False.
    :type filter_cities: bool, optional
    """

    def __init__(
        self,
        filter_pollutant: bool = True,
        filter_non_null_values: bool = True,
        filter_extreme_values: bool = True,
        filter_no_coordinates: bool = True,
        filter_countries: bool = False,
        filter_cities: bool = False,
        countries: List[str] = None,
        cities: List[str] = None,
    ):
        self.filter_pollutant = filter_pollutant
        self.filter_non_null_values = filter_non_null_values
        self.filter_extreme_values = filter_extreme_values
        self.filter_no_coordinates = filter_no_coordinates
        self.filter_countries = filter_countries
        self.filter_cities = filter_cities
        self.countries = countries
        self.cities = cities

    @classmethod
    def from_options(cls, filters: list) -> "Preprocess":
        """
        Create a Preprocess instance with specified filters enabled.

        :param filters: A list of filters to enable.
        :type filters: list
        :return: An instance of Preprocess.
        :rtype: Preprocess
        """
        filter_default = dict.fromkeys(
            [
                "filter_pollutant",
                "filter_non_null_values",
                "filter_extreme_values",
                "filter_no_coordinates",
                "filter_countries",
                "filter_cities",
            ],
            False,
        )
        for filter_ in filters:
            filter_default[filter_] = True
        return cls(**filter_default)

    def execute(
        self, input_df: pd.DataFrame, source: str, **kwargs
    ) -> pd.DataFrame:
        """
        Preprocess raw input data by filtering and transforming the dataset.

        :param input_df: The unprocessed raw data in a DataFrame.
        :type input_df: pd.DataFrame
        :param source: The source of the data, used to apply specific transformations.
        :type source: str
        :return: The processed data after all steps have been applied sequentially.
        :rtype: pd.DataFrame
        """
        input_df = self.get_timestamps(input_df, source)
        input_df = self.extract_coordinates(input_df, source)
        return (
            input_df.pipe(self.filter_data)
            .pipe(self.validate_point)
            .pipe(self.dict_cols_to_json)
        )

    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply a series of filters to the DataFrame based on the enabled options.

        :param df: The DataFrame to filter.
        :type df: pd.DataFrame
        :return: The filtered DataFrame.
        :rtype: pd.DataFrame
        """
        if self.filter_pollutant:
            print(f"Before pollutant filter:\n{df}")
            df = Filter.filter_pollutant(
                df,
                CohortBuilderConfig.TARGET_VARIABLE,
            )
            print(f"After pollutant filter:\n{df}")
        if self.filter_no_coordinates:
            print(f"Before no_coordinates filter:\n{df}")
            df = df.pipe(Filter.filter_no_coordinates)
            print(f"After no_coordinates filter:\n{df}")
        if self.filter_extreme_values:
            print(f"Before extreme_values filter:\n{df}")
            df = df.pipe(Filter.filter_extreme_values)
            print(f"After extreme_values filter:\n{df}")
        if self.filter_non_null_values:
            print(f"Before non_null_values filter:\n{df}")
            df = df.pipe(Filter.filter_non_null_values)
            print(f"After non_null_values filter:\n{df}")
        if self.filter_countries:
            print(f"Before countries filter:\n{df}")
            df = df.pipe(Filter.filter_countries, countries=self.countries)
            print(f"After countries filter:\n{df}")
        if self.filter_cities:
            print(f"Before cities filter:\n{df}")
            df = df.pipe(Filter.filter_cities, cities=self.cities)
            print(f"After cities filter:\n{df}")
        return df

    def get_timestamps(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Extract timestamps from the data and add 'timestamp_utc' and 'timestamp_local' columns.

        :param df: The DataFrame containing date information.
        :type df: pd.DataFrame
        :param source: The source of the data, used to apply specific transformations.
        :type source: str
        :return: The DataFrame with added timestamp columns.
        :rtype: pd.DataFrame
        """
        logging.info("Extracting datetime")
        if source == "openaq-aws":
            df = df.apply(
                lambda row: self._extract_timestamp_from_aws(row),
                axis=1,
            )
        else:
            df = df.apply(
                lambda row: self._extract_timestamp_from_api(row),
                axis=1,
            )
        return df

    def _extract_timestamp_from_aws(self, row: pd.Series) -> pd.Series:
        """
        Extract timezone into "utc" and "local" timezone columns from AWS data.

        :param row: A row from the DataFrame containing date information.
        :type row: pd.Series
        :return: The updated row with 'timestamp_utc' and 'timestamp_local' columns.
        :rtype: pd.Series
        """
        utc_time_str = re.search(r"(?<=utc=)(.*?)(?=,)", row["date"]).group(0)
        local_time_str = re.search(
            r"(?<=local=)(.*?)(?=})", row["date"]
        ).group(0)

        utc_time = datetime.fromisoformat(utc_time_str).replace(
            tzinfo=timezone.utc
        )
        local_time = datetime.fromisoformat(local_time_str)

        row["timestamp_utc"] = utc_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        row["timestamp_local"] = local_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        return row

    def _extract_timestamp_from_api(self, row: pd.Series) -> pd.Series:
        """
        Extract timezone into "utc" and "local" timezone columns from API data.

        :param row: A row from the DataFrame containing date information.
        :type row: pd.Series
        :return: The updated row with 'timestamp_utc' and 'timestamp_local' columns.
        :rtype: pd.Series
        """
        if isinstance(row["date"], str):
            row["date"] = json.loads(row["date"])

        utc_time = row["date"]["utc"]
        local_time = row["date"]["local"]

        # Parse 'utc' time
        if utc_time.endswith("Z"):
            utc_time = utc_time[:-1] + "+00:00"
        row["timestamp_utc"] = (
            datetime.fromisoformat(utc_time)
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        # Parse 'local' time
        row["timestamp_local"] = datetime.fromisoformat(local_time).strftime(
            "%Y-%m-%dT%H:%M:%S.%f%z"
        )

        return row

    def extract_coordinates(
        self, df: pd.DataFrame, source: str
    ) -> pd.DataFrame:
        """
        Extract coordinates into 'x' and 'y' columns from point objects in 'pnt'.
        Filters out rows with invalid point representations.

        :param df: The DataFrame containing coordinate information.
        :type df: pd.DataFrame
        :param source: The source of the data, used to apply specific transformations.
        :type source: str
        :return: The DataFrame with extracted coordinates.
        :rtype: pd.DataFrame
        """
        logging.info("Extracting coordinates")
        if source == "openaq-aws":
            df = df.apply(
                lambda row: self._extract_lat_lng_from_aws(row), axis=1
            )
        else:
            df = df.apply(
                lambda row: self._extract_lat_lng_from_api(row), axis=1
            )

        return df

    def validate_point(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate the geometry of points and filter out invalid points.

        :param df: The DataFrame containing point geometries.
        :type df: pd.DataFrame
        :return: The DataFrame with valid points.
        :rtype: pd.DataFrame
        """
        df["point_is_valid"] = df.pnt.apply(
            lambda x: not x.is_empty and isinstance(x, Point)
        )

        if not all(df.point_is_valid):
            num_invalid_pnts = len(df[~df.point_is_valid])
            logging.info(
                f"There were {num_invalid_pnts} rows with invalid points and"
                " were filtered out"
            )

        df_valid = df[df.point_is_valid]
        return df_valid.drop(["point_is_valid"], axis=1)

    def _extract_lat_lng_from_aws(self, row: pd.Series) -> pd.Series:
        """
        Extract latitude and longitude from AWS data using regex.

        :param row: A row from the DataFrame containing coordinate information.
        :type row: pd.Series
        :return: The updated row with 'x' and 'y' columns.
        :rtype: pd.Series
        """
        row["y"] = float(
            re.search("(?<=latitude=)(.*)(?=,)", row["coordinates"]).group(0)
        )
        row["x"] = float(
            re.search("(?<=longitude=)(.*)(?=})", row["coordinates"]).group(0)
        )

        return self._check_valid_create_pnt(row)

    def _extract_lat_lng_from_api(self, row: pd.Series) -> pd.Series:
        """
        Extract latitude and longitude from API data.

        :param row: A row from the DataFrame containing coordinate information.
        :type row: pd.Series
        :return: The updated row with 'x' and 'y' columns.
        :rtype: pd.Series
        """
        if isinstance(row["coordinates"], str):
            row["coordinates"] = json.loads(row["coordinates"])
        row["y"] = float(row["coordinates"]["latitude"])
        row["x"] = float(row["coordinates"]["longitude"])
        return self._check_valid_create_pnt(row)

    def _check_valid_create_pnt(self, row: pd.Series) -> pd.Series:
        """
        Check if the point is valid and create a Point object.

        :param row: A row from the DataFrame containing coordinate information.
        :type row: pd.Series
        :return: The updated row with a 'pnt' column containing the Point object.
        :rtype: pd.Series
        """
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=ShapelyDeprecationWarning
            )
            row["pnt"] = Point(row["x"], row["y"])
            return row

    def dict_cols_to_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert dictionary columns in the DataFrame to JSON strings.

        :param df: The DataFrame containing columns with dictionary values.
        :type df: pd.DataFrame
        :return: The DataFrame with dictionary columns converted to JSON strings.
        :rtype: pd.DataFrame
        """
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else x
                )
        return df

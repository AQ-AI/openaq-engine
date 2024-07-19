import json
import logging
import re
import warnings
from datetime import datetime, timezone

import pandas as pd
from shapely.errors import ShapelyDeprecationWarning
from shapely.geometry import Point
from src.preprocessing.filter import Filter

from config.model_settings import CohortBuilderConfig


class Preprocess:
    def __init__(
        self,
        filter_pollutant: bool = True,
        filter_non_null_values: bool = True,
        filter_extreme_values: bool = True,
        filter_no_coordinates: bool = True,
        filter_countries: bool = False,
        filter_cities: bool = False,
    ):
        self.filter_pollutant = filter_pollutant
        self.filter_non_null_values = filter_non_null_values
        self.filter_extreme_values = filter_extreme_values
        self.filter_no_coordinates = filter_no_coordinates
        self.filter_countries = filter_countries
        self.filter_cities = filter_cities

    @classmethod
    def from_options(cls, filters) -> "Preprocess":
        filter_default = dict.fromkeys(
            [
                "filter_pollutant",
                "filter_non_null_values",
                "filter_extreme_values",
                "filter_no_coordinates",
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
        Preprocess raw input data by filtering for specific pollutants,
        cleaning columns and extracting location.

        Parameters
        ----------
        input_df : pd.DataFrame
            Unprocessed raw data in a dataframe

        Returns
        -------
        pd.DataFrame
            Processed data after all processing steps have been applied sequentially
        """
        input_df = self.get_timestamps(input_df, source)
        input_df = self.extract_coordinates(input_df, source)
        return (
            input_df.pipe(self.filter_data)
            .pipe(self.validate_point)
            .pipe(self.dict_cols_to_json)
        )

    def filter_data(self, df: pd.DataFrame):
        if self.filter_pollutant:
            df = Filter.filter_pollutant(
                df,
                CohortBuilderConfig.TARGET_VARIABLE,
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering for specific pollutant:
                {len(df)}"""
            )
        if self.filter_no_coordinates:
            df = df.pipe(Filter.filter_no_coordinates)
            logging.info(
                f"""Total number of pollutant values left after
                filtering no coordinates {len(df)}"""
            )
        if self.filter_extreme_values:
            df = df.pipe(Filter.filter_extreme_values)
            logging.info(
                f"""Total number of pollutant values left after
                filtering extreme values {len(df)}"""
            )
        if self.filter_non_null_values:
            df = df.pipe(Filter.filter_non_null_values)
            logging.info(
                f"""Total number of pollutant values left after
                filtering non-null values : {len(df)}"""
            )
        if self.filter_countries:
            df = df.pipe(Filter.filter_countries)
            logging.info(
                f"""Total number of pollutant values left after
                filtering countries: {len(df)}"""
            )
        if self.filter_cities:
            df = df.pipe(Filter.filter_cities)
            logging.info(
                f"""Total number of pollutant values left after
                filtering cities: {len(df)}"""
            )
        return df

    def get_timestamps(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Extract timezone into "utc" and "local" timezone columns.
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
        Extract timezone into "utc" and "local" timezone columns.
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
        Extract timezone into "utc" and "local" timezone columns from dict.
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
        """
        logging.info("Extracting coordinates")
        # Filter out any invalid points
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
        """Filters invalid geometries"""
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
        """Regex extraction of latitude and longtitude from string"""
        row["y"] = float(
            re.search("(?<=latitude=)(.*)(?=,)", row["coordinates"]).group(0)
        )
        row["x"] = float(
            re.search("(?<=longitude=)(.*)(?=})", row["coordinates"]).group(0)
        )

        return self._check_valid_create_pnt(row)

    def _extract_lat_lng_from_api(self, row: pd.Series) -> pd.Series:
        """Extraction of latitude and longitude from dict"""
        if isinstance(row["coordinates"], str):
            row["coordinates"] = json.loads(row["coordinates"])
        row["y"] = float(row["coordinates"]["latitude"])
        row["x"] = float(row["coordinates"]["longitude"])
        return self._check_valid_create_pnt(row)

    def _check_valid_create_pnt(self, row: pd.Series) -> pd.Series:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=ShapelyDeprecationWarning
            )
            row["pnt"] = Point(row["x"], row["y"])
            return row

    def dict_cols_to_json(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else x
                )
        return df

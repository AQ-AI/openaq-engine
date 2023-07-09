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
            try:
                df = Filter.filter_pollutant(
                    df,
                    CohortBuilderConfig.TARGET_VARIABLE,
                )
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering for specific pollutant:
                    {len(df)}"""
                )
            except AttributeError:
                return pd.DataFrame()
        if self.filter_no_coordinates:
            try:

                df = df.pipe(Filter.filter_no_coordinates)
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering no coordinates {len(df)}"""
                )
            except AttributeError:
                return pd.DataFrame()
        if self.filter_extreme_values:
            try:

                df = df.pipe(Filter.filter_extreme_values)
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering extreme values {len(df)}"""
                )
            except AttributeError:
                return pd.DataFrame()
        if self.filter_non_null_values:
            try:
                df = df.pipe(Filter.filter_non_null_values)
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering non-null values : {len(df)}"""
                )
            except AttributeError:
                logging.info(f"""No Valid pollutants from {len(df)} points""")
                pass
        if self.filter_countries:
            try:

                df = df.pipe(Filter.filter_countries)
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering countries: {len(df)}"""
                )
            except AttributeError:
                logging.info(f"""No Valid countries from {len(df)} points""")
                pass
        if self.filter_cities:
            try:

                df = df.pipe(Filter.filter_cities)
                logging.info(
                    f"""Total number of pollutant values left after
                    filtering cities: {len(df)}"""
                )
            except AttributeError:
                logging.info(f"""No Valid cities from {len(df)} points""")
                pass
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
        row["timestamp_utc"] = (
            datetime.fromisoformat(
                re.search("(?<=utc=)(.*)(?=,)", row["date"]).group(0)[:-1]
            )
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        row["timestamp_local"] = (
            datetime.fromisoformat(
                re.search("(?<=local=)(.*)(?=})", row["date"]).group(0),
            )
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S%z")
        )
        return row

    def _extract_timestamp_from_api(self, row: pd.Series) -> pd.Series:
        """
        Extract timezone into "utc" and "local" timezone columns from dict.
        """
        row["timestamp_utc"] = (
            datetime.fromisoformat(row["date"]["utc"])
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        row["timestamp_local"] = (
            datetime.fromisoformat(row["date"]["local"])
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S%z")
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
        """filters invalid geometries"""
        try:

            df["point_is_valid"] = df.pnt.apply(
                lambda x: x.wkt != "POINT EMPTY"
            )

            if not all(df.point_is_valid):
                num_invalid_pnts = len(df[~df.point_is_valid])
                logging.info(
                    f"There were {num_invalid_pnts} rows with invalid points"
                    " and were filtered out"
                )
            df_valid = df[df.point_is_valid]
            return df_valid.drop(["pnt", "point_is_valid"], axis=1)
        except AttributeError:
            logging.info(f"None of the {len(df)} rows had calid points")
            pass

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
        """Extraction of latitude and longtitude from dict"""
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
        """Dumps cols containing dicts to json"""
        try:
            for i in df.columns:
                if isinstance(df[i][1], dict):
                    df[i] = list(map(lambda x: json.dumps(x), df[i]))

            return df
        except AttributeError:
            return pd.DataFrame()

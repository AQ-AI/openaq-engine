import logging
import re
import warnings

import pandas as pd
from config.model_settings import CohortBuilderConfig
from shapely.errors import ShapelyDeprecationWarning
from shapely.geometry import Point
from src.preprocessing.filter import Filter


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

    def execute(self, input_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
        return (
            input_df.pipe(self.filter_data)
            # .pipe(self.shift_columns)
            .pipe(self.extract_coordinates)
        )

    def filter_data(self, df: pd.DataFrame):
        if self.filter_pollutant:
            df = Filter.filter_pollutant(
                df,
                CohortBuilderConfig.POLLUTANT_TO_PREDICT,
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
            df = df.pipe(Filter.filter_extreme_pollution_values)
            logging.info(
                f"""Total number of pollutant values left after
                filtering extreme values {len(df)}"""
            )
        if self.filter_non_null_values:
            df = df.pipe(Filter.filter_non_null_pm25_values)
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

    def extract_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract coordinates into 'x' and 'y' columns from point objects in 'pnt'.
        Filters out rows with invalid point representations.
        """
        logging.info("Extracting coordinates")
        # Filter out any invalid points
        df = df.apply(lambda row: self._extract_lat_lng(row), axis=1)
        df["point_is_valid"] = df.pnt.apply(lambda x: x.wkt != "POINT EMPTY")

        if not all(df.point_is_valid):
            num_invalid_pnts = len(df[~df.point_is_valid])
            logging.info(
                f"There were {num_invalid_pnts} rows with invalid points and were filtered out"
            )

        df_valid = df[df.point_is_valid]
        return df_valid.drop(["pnt", "point_is_valid"], axis=1)

    def _extract_lat_lng(self, row):
        row["y"] = float(
            re.search("(?<=latitude=)(.*)(?=,)", row["coordinates"]).group(0)
        )
        row["x"] = float(
            re.search("(?<=longitude=)(.*)(?=})", row["coordinates"]).group(0)
        )
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)
            row["pnt"] = Point(row["x"], row["y"])
            return row

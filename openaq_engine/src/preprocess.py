import logging

import pandas as pd
from config.model_settings import CohortBuilderConfig
from src.preprocessing.cohort_filter import Filter


class Preprocess:
    def __init__(
        self,
        filter_pollutant: bool = True,
        filter_non_null_values: bool = True,
        filter_extreme_values: bool = True,
        filter_countries: bool = False,
        filter_cities: bool = False,
    ):
        self.filter_pollutant = filter_pollutant
        self.filter_non_null_values = filter_non_null_values
        self.filter_extreme_values = filter_extreme_values
        self.filter_countries = filter_countries
        self.filter_cities = filter_cities

    @classmethod
    def from_options(cls, filters) -> "Preprocess":
        filter_default = dict.fromkeys(
            [
                "filter_pollutant",
                "filter_non_null_values",
                "filter_extreme_values",
            ],
            False,
        )
        for filter_ in filters:
            filter_default[filter_] = True
        return cls(**filter_default)

    def execute(self, training_validation_df: pd.DataFrame, **kwargs):
        preprocessed_training = self.preprocess_data(training_validation_df)

        return preprocessed_training

    def preprocess_data(self, training_validation_df: pd.DataFrame):
        if self.filter_pollutant:
            training_validation_df = Filter.filter_pollutant(
                training_validation_df,
                CohortBuilderConfig.POLLUTANT_TO_PREDICT,
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering for specific pollutant:
                {len(training_validation_df)}"""
            )
        if self.filter_extreme_values:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_extreme_pollution_values
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering extreme values {len(training_validation_df)}"""
            )
        if self.filter_non_null_values:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_non_null_pm25_values
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering non-null values : {len(training_validation_df)}"""
            )
        if self.filter_countries:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_countries
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering countries: {len(training_validation_df)}"""
            )
        if self.filter_cities:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_cities
            )
            logging.info(
                f"""Total number of pollutant values left after
                filtering cities: {len(training_validation_df)}"""
            )
        return training_validation_df

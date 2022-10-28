import logging
import pandas as pd
from src.preprocessing.cohort_filter import Filter


class Preprocess:
    def __init__(
        self,
        filter_non_null: bool = True,
        filter_extreme: bool = True,
    ):
        self.filter_non_null = filter_non_null
        self.filter_extreme = filter_extreme

    @classmethod
    def from_options(cls, filters) -> "Preprocess":
        filter_default = dict.fromkeys(["filter_non_null", "filter_extreme"], False)
        for filter_ in filters:
            filter_default[filter_] = True
        return cls(**filter_default)

    def execute(self, training_validation_df: pd.DataFrame, **kwargs):
        preprocessed_training = self.preprocess_data(training_validation_df)

        return preprocessed_training

    def preprocess_data(self, training_validation_df: pd.DataFrame):
        if self.filter_non_null:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_non_null_pm25_values
            )
            logging.info(
                f"""Total number of patients left after
                filtering pregnancies : {len(training_validation_df)}"""
            )
        if self.filter_extreme:
            training_validation_df = training_validation_df.pipe(
                Filter.filter_extreme_pollution_values
            )
            logging.info(
                f"""Total number of patients left after
                filtering children {len(training_validation_df)}"""
            )

        return training_validation_df

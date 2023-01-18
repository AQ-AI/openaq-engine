from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from src.features.satellite._ee_data import EEFeatures
from src.utils.utils import write_to_db

from config.model_settings import BuildFeaturesConfig, EEConfig


class BuildFeatureBase(ABC):
    def __init__(self, target_col: str):
        self.target_col = target_col

    @abstractmethod
    def execute(self, *args: Any) -> pd.DataFrame:
        ...


class BuildFeaturesRandomForest(BuildFeatureBase):
    def __init__(
        self,
        categorical_features: Dict[str, List[Any]],
        all_model_features: Optional[List[str]],
    ) -> None:
        self.categorical_features = categorical_features
        self._all_model_features = all_model_features
        super().__init__(BuildFeaturesConfig.TARGET_COL)

    @classmethod
    def from_dataclass_config(
        cls, config: BuildFeaturesConfig
    ) -> "BuildFeaturesRandomForest":
        return cls(
            categorical_features=config.CATEGORICAL_FEATURES,
            all_model_features=config.ALL_MODEL_FEATURES,
        )

    def execute(self, engine, cohort_df) -> pd.DataFrame:
        df = self._add_ee_features(cohort_df)
        df = self._change_to_categorical_type(df)
        self._results_to_db(df, engine)

        (
            df_train,
            df_valid,
            feature_train_id,
            feature_valid_id,
            train_labels,
            validation_labels,
        ) = self._split_train_valid(cohort_df, df)

        return (
            df_train,
            df_valid,
            feature_train_id,
            feature_valid_id,
            train_labels,
            validation_labels,
        )

    @property
    def all_model_features(self):
        return self._all_model_features

    @all_model_features.setter
    def all_model_features(self, features: List[str]):
        if not all(type(feat) == str for feat in features):
            raise ValueError("All the feature names should be strings!")
        self._all_model_features = features

    def _add_ee_features(self, df):
        return EEFeatures.from_dataclass_config(EEConfig()).execute(
            df, save_images=False
        )

    def _add_year(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(year=lambda df: pd.to_datetime(df.day).dt.year)

    def _change_to_categorical_type(self, df: pd.DataFrame) -> pd.DataFrame:
        for cat_col in self.categorical_features:
            df.loc[:, cat_col] = df[cat_col].astype("category")

        return df

    def _results_to_db(self, features_df, engine):
        """Write model results to the database for all cohorts"""

        write_to_db(
            features_df,
            engine,
            "features",
            "public",
            "append",
        )

    def _split_train_valid(self, cohort_df, df):
        print(df)
        df = df.merge(
            cohort_df[["locationId", "cohort_type", "value"]],
            how="left",
            left_on="location_id",
            right_on="locationId",
        )

        df_train = df.loc[df["cohort_type"] == "training"]
        df_valid = df.loc[df["cohort_type"] == "validation"]
        train_ids, valid_ids = self._get_uniqueids(df_train, df_valid)
        train_labels = df_train[["value"]]
        validation_labels = df_valid[["value"]]
        return (
            df_train,
            df_valid,
            train_ids,
            valid_ids,
            train_labels,
            validation_labels,
        )

    def _get_uniqueids(self, df_train, df_valid):
        train_ids = df_train[["location_id"]].reset_index(drop=True)

        valid_ids = df_valid[["location_id"]].reset_index(drop=True)

        return train_ids, valid_ids

    def _filter_labels(self, cohort_df, labels_df):
        filtered_labels_df = labels_df.merge(
            cohort_df[["locationId"]],
            how="right",
            on="locationId",
        )
        print(
            "len(labels_df)",
            len(labels_df),
            "len(filtered_labels_df)",
            len(filtered_labels_df),
        )
        return filtered_labels_df


def get_feature_builder(algorithm: str) -> Type[BuildFeatureBase]:
    if algorithm == "RFC":
        return BuildFeaturesRandomForest

    else:
        raise ValueError(
            "The algorithm provided has no registered feature builder!"
        )

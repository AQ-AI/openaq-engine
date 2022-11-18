from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from config.model_settings import FeatureConfig


class BuildFeatureBase(ABC):
    def __init__(self, target_col: str):
        self.target_col = target_col

    @abstractmethod
    def build(self, *args: Any) -> pd.DataFrame:
        ...


class BuildFeaturesRandomForest(BuildFeatureBase):
    def __init__(
        self,
        target_col: str,
        categorical_features: Dict[str, List[Any]],
        all_model_features: Optional[List[str]] = None,
    ):
        super().__init__(target_col)
        self.categorical_features = categorical_features
        self._all_model_features = (
            all_model_features or FeatureConfig.ALL_MODEL_FEATURES
        )

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self._add_year).pipe(self._change_to_categorical_type)[
            self.all_model_features
        ]

    @property
    def all_model_features(self):
        return self._all_model_features

    @all_model_features.setter
    def all_model_features(self, features: List[str]):
        if not all(type(feat) == str for feat in features):
            raise ValueError("All the feature names should be strings!")
        self._all_model_features = features

    def _add_year(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(year=lambda df: pd.to_datetime(df.listed_at).dt.year)

    def _change_to_categorical_type(self, df: pd.DataFrame) -> pd.DataFrame:
        for cat_col in self.categorical_features:
            df.loc[:, cat_col] = df[cat_col].astype("category")

        return df


def get_feature_builder(algorithm: str) -> Type[BuildFeatureBase]:
    if algorithm == "RFC":
        return BuildFeaturesRandomForest

    else:
        raise ValueError(
            "The algorithm provided has no registered feature builder!"
        )

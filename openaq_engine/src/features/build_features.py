from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from src.features.satellite._ee_data import EEFeatures
from src.utils.utils import get_data, write_to_db

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

    def execute(self, engine, country, pollutant) -> pd.DataFrame:
        if country == "WO":
            cohort_query = (
                f"""select * from "cohorts" where parameter="{pollutant}";"""
            )
        else:
            cohort_query = f"""select * from "cohorts" where parameter='{pollutant}' and country='{country}';"""

        df = get_data(cohort_query)
        df = df.pipe(self._add_ee_features).pipe(
            self._change_to_categorical_type
        )[self.all_model_features]
        self._results_to_db(df, engine)

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
            "replace",
        )


def get_feature_builder(algorithm: str) -> Type[BuildFeatureBase]:
    if algorithm == "RFC":
        return BuildFeaturesRandomForest

    else:
        raise ValueError(
            "The algorithm provided has no registered feature builder!"
        )

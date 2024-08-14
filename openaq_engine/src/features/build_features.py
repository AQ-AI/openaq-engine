from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

import pandas as pd

from config.model_settings import BuildFeaturesConfig, EEConfig
from openaq_engine.src.features.satellite._ee_data import EEFeatures
from openaq_engine.src.utils.utils import write_to_db


class BuildFeatureBase(ABC):
    """
    Abstract base class for building features for machine learning models.

    Parameters
    ----------
    target_col : str
        The name of the target column for which features are being built.
    """

    def __init__(self, target_col: str):
        self.target_col = target_col

    @abstractmethod
    def execute(self, *args: Any) -> pd.DataFrame:
        """
        Abstract method to execute the feature building process.

        Parameters
        ----------
        args : Any
            Additional arguments required for feature building.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the built features.
        """
        pass


class BuildFeaturesRandomForest(BuildFeatureBase):
    """
    Class for building features specifically for Random Forest models.

    Parameters
    ----------
    categorical_features : Dict[str, List[Any]]
        A dictionary of categorical features.
    all_model_features : Optional[List[str]]
        A list of all model features, default is None.
    """

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
        """
        Create an instance of BuildFeaturesRandomForest from a configuration dataclass.

        Parameters
        ----------
        config : BuildFeaturesConfig
            The configuration dataclass.

        Returns
        -------
        BuildFeaturesRandomForest
            An instance of BuildFeaturesRandomForest.
        """
        return cls(
            categorical_features=config.CATEGORICAL_FEATURES,
            all_model_features=config.ALL_MODEL_FEATURES,
        )

    def execute(self, engine: Any, cohort_df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the feature building process for the Random Forest model.

        Parameters
        ----------
        engine : Any
            The database engine to save the features.
        cohort_df : pd.DataFrame
            The DataFrame containing the cohort data.

        Returns
        -------
        pd.DataFrame
            A tuple containing the training and validation features and labels.
        """
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
    def all_model_features(self) -> Optional[List[str]]:
        """
        Get all model features.

        Returns
        -------
        Optional[List[str]]
            A list of all model features.
        """
        return self._all_model_features

    @all_model_features.setter
    def all_model_features(self, features: List[str]) -> None:
        """
        Set the list of all model features.

        Parameters
        ----------
        features : List[str]
            A list of feature names.

        Raises
        ------
        ValueError
            If any feature name is not a string.
        """
        if not all(isinstance(feat, str) for feat in features):
            raise ValueError("All the feature names should be strings!")
        self._all_model_features = features

    def _add_ee_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add Earth Engine (EE) features to the DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to which EE features will be added.

        Returns
        -------
        pd.DataFrame
            The DataFrame with EE features added.
        """
        return EEFeatures.from_dataclass_config(EEConfig()).execute(
            df, save_images=False
        )

    def _add_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a year column to the DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to which the year column will be added.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the year column added.
        """
        return df.assign(year=lambda df: pd.to_datetime(df.day).dt.year)

    def _change_to_categorical_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert specified columns to categorical type.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the columns to be converted.

        Returns
        -------
        pd.DataFrame
            The DataFrame with specified columns converted to categorical type.
        """
        for cat_col in self.categorical_features:
            df.loc[:, cat_col] = df[cat_col].astype("category")

        return df

    def _results_to_db(self, features_df: pd.DataFrame, engine: Any) -> None:
        """
        Write the feature DataFrame to the database.

        Parameters
        ----------
        features_df : pd.DataFrame
            The DataFrame containing the features.
        engine : Any
            The database engine for saving the features.
        """
        write_to_db(
            features_df,
            engine,
            "features",
            "public",
            "append",
        )

    def _split_train_valid(
        self, cohort_df: pd.DataFrame, df: pd.DataFrame
    ) -> tuple:
        """
        Split the DataFrame into training and validation sets.

        Parameters
        ----------
        cohort_df : pd.DataFrame
            The DataFrame containing the cohort data.
        df : pd.DataFrame
            The DataFrame containing the features.

        Returns
        -------
        tuple
            A tuple containing the training and validation features and labels.
        """
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

    def _get_uniqueids(
        self, df_train: pd.DataFrame, df_valid: pd.DataFrame
    ) -> tuple:
        """
        Get unique IDs for training and validation sets.

        Parameters
        ----------
        df_train : pd.DataFrame
            The DataFrame containing the training data.
        df_valid : pd.DataFrame
            The DataFrame containing the validation data.

        Returns
        -------
        tuple
            A tuple containing the unique IDs for the training and validation sets.
        """
        train_ids = df_train[["location_id"]].reset_index(drop=True)
        valid_ids = df_valid[["location_id"]].reset_index(drop=True)
        return train_ids, valid_ids

    def _filter_labels(
        self, cohort_df: pd.DataFrame, labels_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Filter labels based on the cohort DataFrame.

        Parameters
        ----------
        cohort_df : pd.DataFrame
            The DataFrame containing the cohort data.
        labels_df : pd.DataFrame
            The DataFrame containing the labels.

        Returns
        -------
        pd.DataFrame
            The filtered labels DataFrame.
        """
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
    """
    Get the feature builder class based on the algorithm name.

    Parameters
    ----------
    algorithm : str
        The name of the algorithm for which features need to be built.

    Returns
    -------
    Type[BuildFeatureBase]
        The feature builder class corresponding to the algorithm.

    Raises
    ------
    ValueError
        If the algorithm is not recognized.
    """
    if algorithm == "RFC":
        return BuildFeaturesRandomForest
    else:
        raise ValueError(
            "The algorithm provided has no registered feature builder!"
        )

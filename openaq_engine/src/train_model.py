import itertools
import logging
import os
import string
from datetime import datetime
from typing import Any, List, Optional

import psutil
from joblib import Parallel, delayed, dump
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.multioutput import MultiOutputClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sqlalchemy import text

from config.model_settings import (
    EEConfig,
    HyperparamConfig,
    ModelTrainerConfig,
)

logging.basicConfig(level=logging.INFO)


class ModelTrainer:
    """
    A class for training machine learning models using various algorithms.

    :param model_names_list: A list of model names to be trained.
    :type model_names_list: list
    :param random_state: The random state for reproducibility.
    :type random_state: int
    :param id_cols_to_remove: A list of column names to be removed from the dataset.
    :type id_cols_to_remove: list
    :param all_model_features: A list of all features to be used for model training, defaults to None.
    :type all_model_features: list, optional
    """

    def __init__(
        self,
        model_names_list: List[str],
        random_state: int,
        id_cols_to_remove: List[str],
        all_model_features: Optional[List[str]] = None,
    ) -> None:
        self.model_names_list = model_names_list
        self.random_state = random_state
        self.id_cols_to_remove = id_cols_to_remove
        self.all_model_features = all_model_features

    @classmethod
    def from_dataclass_config(
        cls, config: ModelTrainerConfig
    ) -> "ModelTrainer":
        """
        Create a ModelTrainer instance from a configuration dataclass.

        :param config: The configuration dataclass.
        :type config: ModelTrainerConfig
        :return: An instance of ModelTrainer.
        :rtype: ModelTrainer
        """
        return cls(
            model_names_list=config.MODEL_NAMES_LIST,
            random_state=config.RANDOM_STATE,
            id_cols_to_remove=config.ID_COLS_TO_REMOVE,
            all_model_features=list(
                itertools.chain(
                    *[x[1] for x in list(EEConfig().ALL_SATELLITES)]
                )
            ),
        )

    def train_all_models(
        self,
        cohort_id: str,
        X_train: Any,
        Y_train: Any,
        model_path: str,
        run_date: datetime,
        engine: Any,
    ) -> List[tuple]:
        """
        Train all specified models and save each trained model to the server.

        :param cohort_id: The cohort ID for the training dataset.
        :type cohort_id: str
        :param X_train: The training data features.
        :type X_train: Any
        :param Y_train: The training data labels.
        :type Y_train: Any
        :param model_path: The path where the trained models will be saved.
        :type model_path: str
        :param run_date: The date and time of the training run.
        :type run_date: datetime
        :param engine: The database engine for saving model metadata.
        :type engine: Any
        :return: A list of tuples containing model IDs, model names, and cohort IDs.
        :rtype: list
        """
        logging.info("Training all models")
        logging.info(Y_train)
        model_output = []
        for model in self.model_names_list:

            model_params = HyperparamConfig.MODEL_HYPERPARAMS[model]
            hyperparams = itertools.product(*list(model_params.values()))

            if model == "DTC":
                model_output += self._parallelize_dtc(
                    cohort_id,
                    model,
                    X_train,
                    Y_train,
                    model_path,
                    run_date,
                    hyperparams,
                    engine,
                )
            else:
                for hp in hyperparams:
                    model_output += [
                        self.execute_one_model(
                            cohort_id,
                            model,
                            X_train,
                            Y_train,
                            model_path,
                            run_date,
                            hp,
                            engine,
                        )
                    ]

        return model_output

    def execute_one_model(
        self,
        cohort_id: str,
        model_name: str,
        X_train: Any,
        Y_train: Any,
        model_path: str,
        run_date: datetime,
        hp: tuple,
        engine: Any,
    ) -> tuple:
        """
        Train a single model and save the trained model and its metadata.

        :param cohort_id: The cohort ID for the training dataset.
        :type cohort_id: str
        :param model_name: The name of the model to be trained.
        :type model_name: str
        :param X_train: The training data features.
        :type X_train: Any
        :param Y_train: The training data labels.
        :type Y_train: Any
        :param model_path: The path where the trained model will be saved.
        :type model_path: str
        :param run_date: The date and time of the training run.
        :type run_date: datetime
        :param hp: The hyperparameters for the model.
        :type hp: tuple
        :param engine: The database engine for saving model metadata.
        :type engine: Any
        :return: A tuple containing the model ID, model name, and cohort ID.
        :rtype: tuple
        """
        logging.info(f"Training model {model_name} with hyperparameters {hp}")
        X_train = X_train[self.all_model_features]
        text_clf = self.get_train_pipeline(model_name, hp)
        logging.info("Fitting model")
        logging.info(f"Current memory usage: {psutil.virtual_memory()}")
        logging.info(f"Shape of X data: {X_train.shape}")
        logging.info(f"Shape of Y data: {Y_train.shape}")
        X_train = self.get_impute_transformer().fit_transform(X_train)
        X_train = self.get_scaler_transform().fit_transform(X_train)
        train_model = self.fit_model(text_clf, X_train, Y_train)

        hp_id = self._build_hyperparameters_id(model_name, hp)
        model_id, model_set = self._generate_model_id(
            train_model,
            model_name,
            cohort_id,
            hp_id,
        )

        self._save_trained_model(train_model, model_path, model_id, run_date)

        self._generate_model_metadata(
            model_id,
            model_set,
            run_date,
            list(Y_train.columns),
            hp_id,
            engine,
        )
        return model_id, model_name, cohort_id

    def get_train_pipeline(self, model_name: str, hp: tuple) -> Pipeline:
        """
        Create a pipeline for model training based on the model name and hyperparameters.

        :param model_name: The name of the model.
        :type model_name: str
        :param hp: The hyperparameters for the model.
        :type hp: tuple
        :return: A scikit-learn Pipeline object for training the model.
        :rtype: Pipeline
        """
        return Pipeline(
            [
                (f"{model_name}", self._get_model(model_name, hp)),
            ]
        )

    def get_impute_transformer(self) -> ColumnTransformer:
        """
        Create a column transformer for imputing missing values in the dataset.

        :return: A ColumnTransformer object for imputing missing values.
        :rtype: ColumnTransformer
        """
        numeric_pipeline = Pipeline(
            steps=[("impute", SimpleImputer(strategy="mean"))]
        )
        return ColumnTransformer(
            transformers=[
                ("numeric", numeric_pipeline, self.all_model_features)
            ]
        )

    def get_scaler_transform(self) -> StandardScaler:
        """
        Create a standard scaler transformer for normalizing the dataset.

        :return: A StandardScaler object for normalizing the data.
        :rtype: StandardScaler
        """
        return StandardScaler()

    def fit_model(
        self, text_clf: Pipeline, X_train: Any, y_train: Any
    ) -> Pipeline:
        """
        Fit the model to the training data.

        :param text_clf: The model pipeline to be trained.
        :type text_clf: Pipeline
        :param X_train: The training data features.
        :type X_train: Any
        :param y_train: The training data labels.
        :type y_train: Any
        :return: The trained model pipeline.
        :rtype: Pipeline
        """
        return text_clf.fit(X_train, y_train)

    def _drop_id_label_cols(self, df: Any, mlb_categories: List[str]) -> Any:
        """
        Drop ID and label columns from the dataset.

        :param df: The DataFrame from which columns will be dropped.
        :type df: Any
        :param mlb_categories: A list of columns to be retained.
        :type mlb_categories: list
        :return: The DataFrame with ID and label columns removed.
        :rtype: Any
        """
        return df.drop(
            list(mlb_categories) + self.id_cols_to_remove,
            axis=1,
        )

    def _remove_punctuation(self, text_column: str) -> str:
        """
        Remove punctuation from text data.

        :param text_column: The text column from which punctuation will be removed.
        :type text_column: str
        :return: The text without punctuation.
        :rtype: str
        """
        return "".join([i for i in text_column if i not in string.punctuation])

    def _get_model(self, model_name: str, hp: tuple) -> Any:
        """
        Get the appropriate model instance based on the model name and hyperparameters.

        :param model_name: The name of the model.
        :type model_name: str
        :param hp: The hyperparameters for the model.
        :type hp: tuple
        :return: The model instance.
        :rtype: Any
        """
        if model_name == "DTC":
            return DecisionTreeClassifier(
                max_depth=hp[0], random_state=self.random_state
            )
        elif model_name == "RFR":
            return RandomForestRegressor(
                n_jobs=-3,
                n_estimators=hp[0],
                max_depth=hp[1],
                random_state=self.random_state,
            )
        elif model_name == "MNB":
            model = MultinomialNB(alpha=hp[0])
            return MultiOutputClassifier(model)
        elif model_name == "MLR":
            model = LogisticRegression(
                penalty=hp[0], C=hp[1], solver=hp[2], max_iter=hp[3]
            )
            return MultiOutputClassifier(model)
        else:
            logging.info(f"Model name {model_name} does not exist")

    def _generate_model_id(
        self,
        train_model: Pipeline,
        model_name: str,
        cohort_id: str,
        hp_id: str,
    ) -> tuple:
        """
        Generate a unique model ID based on the model name, cohort ID, and hyperparameters.

        :param train_model: The trained model pipeline.
        :type train_model: Pipeline
        :param model_name: The name of the model.
        :type model_name: str
        :param cohort_id: The cohort ID for the training dataset.
        :type cohort_id: str
        :param hp_id: The hyperparameter ID for the model.
        :type hp_id: str
        :return: A tuple containing the model ID and model set identifier.
        :rtype: tuple
        """
        model_name = train_model.named_steps[
            f"{model_name}"
        ].__class__.__name__
        model_id = "_".join(
            list(
                map(
                    self._clean_for_model_id,
                    [model_name, hp_id, cohort_id],
                )
            )
        )
        model_set = "_".join(
            list(
                map(
                    self._clean_for_model_id,
                    [model_name, hp_id],
                )
            )
        )

        return model_id, model_set

    def _clean_for_model_id(self, word: str) -> str:
        """
        Clean and format words for creating a model ID.

        :param word: The word to be cleaned.
        :type word: str
        :return: The cleaned word.
        :rtype: str
        """
        word = str(word).replace("-", "")
        return word.lower()

    def _save_trained_model(
        self,
        train_model: Pipeline,
        model_path: str,
        model_id: str,
        run_date: datetime,
    ) -> None:
        """
        Save the trained model to the specified path.

        :param train_model: The trained model pipeline.
        :type train_model: Pipeline
        :param model_path: The path where the model will be saved.
        :type model_path: str
        :param model_id: The unique model ID.
        :type model_id: str
        :param run_date: The date and time of the training run.
        :type run_date: datetime
        """
        filename = (
            "_".join([model_id, run_date.strftime("%Y%m%d_%H%M%S%f")])
            + ".joblib"
        )
        dump(train_model, os.path.join(model_path, filename))

    def _build_hyperparameters_id(self, model_name: str, hp: tuple) -> str:
        """
        Build a hyperparameter ID string based on the model name and hyperparameters.

        :param model_name: The name of the model.
        :type model_name: str
        :param hp: The hyperparameters for the model.
        :type hp: tuple
        :return: The hyperparameter ID as a string.
        :rtype: str
        """
        if model_name == "DTC":
            return f"max_depth{hp[0]}"
        elif model_name == "RFR":
            return f"n_estimators{hp[0]}_max_depth{hp[1]}"
        elif model_name == "MNB":
            return f"alpha{hp[0]}"
        elif model_name == "MLR":
            return f"penalty{hp[0]}_C{hp[1]}"

    def _generate_model_metadata(
        self,
        model_id: str,
        model_set: str,
        run_date: datetime,
        labels: List[str],
        hp_id: str,
        engine: Any,
    ) -> None:
        """
        Generate and save model metadata to the database.

        :param model_id: The unique model ID.
        :type model_id: str
        :param model_set: The model set identifier.
        :type model_set: str
        :param run_date: The date and time of the training run.
        :type run_date: datetime
        :param labels: The list of labels used in training.
        :type labels: list
        :param hp_id: The hyperparameter ID for the model.
        :type hp_id: str
        :param engine: The database engine for saving model metadata.
        :type engine: Any
        """
        with engine.connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS model_metadata
                            (model_id text,
                            model_set text,
                            features varchar[],
                            labels varchar[],
                            hyperparameters varchar,
                            run_date timestamp)"""
            )

        with engine.connect() as conn:
            logging.info("Inserting model information into database")
            q = text(
                """insert into model_metadata
                (model_id, model_set, features, labels, hyperparameters, run_date)
                    values (:m1, :m2, :f, :l, :h, :r);"""
            )
            conn.execute(
                q,
                m1=model_id,
                m2=model_set,
                f=self.all_model_features,
                l=labels,
                h=hp_id,
                r=run_date,
            )

    def _parallelize_dtc(
        self,
        cohort_id: str,
        model: str,
        X_train: Any,
        Y_train: Any,
        model_path: str,
        run_date: datetime,
        hyperparams: Any,
        engine: Any,
    ) -> List[tuple]:
        """
        Parallelize the training of Decision Tree Classifiers with different hyperparameters.

        :param cohort_id: The cohort ID for the training dataset.
        :type cohort_id: str
        :param model: The model name.
        :type model: str
        :param X_train: The training data features.
        :type X_train: Any
        :param Y_train: The training data labels.
        :type Y_train: Any
        :param model_path: The path where the trained models will be saved.
        :type model_path: str
        :param run_date: The date and time of the training run.
        :type run_date: datetime
        :param hyperparams: The hyperparameters for the Decision Tree Classifier.
        :type hyperparams: Any
        :param engine: The database engine for saving model metadata.
        :type engine: Any
        :return: A list of tuples containing model IDs, model names, and cohort IDs.
        :rtype: list
        """
        return Parallel(n_jobs=-2, backend="threading")(
            delayed(self.execute_one_model)(
                cohort_id,
                model,
                X_train,
                Y_train,
                model_path,
                run_date,
                hp,
                engine,
            )
            for hp in hyperparams
        )

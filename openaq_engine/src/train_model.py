import itertools
import logging
import os
import string
from typing import List, Optional

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
    def __init__(
        self,
        model_names_list: List,
        random_state: int,
        id_cols_to_remove: List,
        # best_model: str,
        # best_model_hyperparams: List,
        all_model_features: Optional[List[str]] = None,
    ) -> None:
        self.model_names_list = model_names_list
        self.random_state = random_state
        self.id_cols_to_remove = id_cols_to_remove
        # self.best_model = best_model
        # self.best_model_hyperparams = best_model_hyperparams
        self.all_model_features = all_model_features

    @classmethod
    def from_dataclass_config(
        cls, config: ModelTrainerConfig
    ) -> "ModelTrainer":
        return cls(
            model_names_list=config.MODEL_NAMES_LIST,
            random_state=config.RANDOM_STATE,
            id_cols_to_remove=config.ID_COLS_TO_REMOVE,
            all_model_features=list(
                itertools.chain(
                    *[x[1] for x in list(EEConfig().ALL_SATELLITES)]
                )
            )
            # best_model=RetrainingConfig().BEST_MODEL,
            # best_model_hyperparams=RetrainingConfig().BEST_MODEL_HYPERPARAMS,
        )

    # def train_best_model(
    #     self, cohort_id, X_train, Y_train, model_path, run_date, schema_type
    # ):
    #     self.get_schema_name(schema_type)
    #     return self.execute_one_model(
    #         cohort_id,
    #         self.best_model,
    #         X_train,
    #         Y_train,
    #         model_path,
    #         run_date,
    #         self.best_model_hyperparams,
    #         engine,
    #     )

    def train_all_models(
        self,
        cohort_id,
        X_train,
        Y_train,
        model_path,
        run_date,
        engine,
    ):
        """Loop through all models and save each trained model to server"""
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
        cohort_id,
        model_name,
        X_train,
        Y_train,
        model_path,
        run_date,
        hp,
        engine,
        X_valid,
    ):
        """This is a docstring that describes the overall function:
        Arguments
        ---------
            model_id : str
                      A model_id that identifies the model
                      `model_name` and `hyperparameters`
            train_model: model
                     model instance to use in training
            model_name: str
                      A name identifying the algorithm

        Returns
        -------
            train_model: model
                     model instance to use in training
            mlb: MultiLabelBinarizer()
                      A label binariser to generate category matrix.
            model_id: str
                      A model_id that identifies the model based on the `cohort_id` and
                      `hyperparameters`"""
        logging.info(f"Training model {model_name} with hyperparameters {hp}")
        X_train = X_train[self.all_model_features]
        X_valid = X_valid[self.all_model_features]
        # split by labels and features
        text_clf = self.get_train_pipeline(model_name, hp)
        logging.info("Fitting model")
        logging.info(f"Current memory usage: {psutil.virtual_memory()}")
        logging.info(f"Shape of X data: {X_train.shape}")
        logging.info(f"Shape of Y data: {Y_train.shape}")
        X_train = self.get_impute_transformer().fit_transform(X_train)
        X_train = self.get_scaler_transform().fit_transform(X_train)
        X_valid = self.get_impute_transformer().fit_transform(X_valid)
        X_valid = self.get_scaler_transform().fit_transform(X_valid)
        train_model = self.fit_model(text_clf, X_train, Y_train)

        hp_id = self._build_hyperparameters_id(model_name, hp)
        # get model_id
        model_id, model_set = self._generate_model_id(
            train_model,
            model_name,
            cohort_id,
            hp_id,
        )

        # write model to server
        self._save_trained_model(train_model, model_path, model_id, run_date)

        # write model metadata
        self._generate_model_metadata(
            model_id,
            model_set,
            run_date,
            list(Y_train.columns),
            hp_id,
            engine,
        )
        return model_id, model_name, cohort_id, X_valid

    def get_train_pipeline(self, model_name, hp):
        """
        Create pipeline based on model name and instantiation

        Arguments
        ---------
            model_name : dict

        Returns
        -------
            pipeline
            model_name : str
        """
        text_clf = Pipeline(
            [
                # ("vect", CountVectorizer()),
                (f"{model_name}", self._get_model(model_name, hp)),
            ]
        )
        return text_clf

    def get_impute_transformer(self):
        numeric_pipeline = Pipeline(
            steps=[("impute", SimpleImputer(strategy="mean"))]
        )
        return ColumnTransformer(
            transformers=[
                ("numeric", numeric_pipeline, self.all_model_features)
            ]
        )

    def get_scaler_transform(self):
        scaler = StandardScaler()
        return scaler

    def fit_model(self, text_clf, X_train, y_train):
        return text_clf.fit(X_train, y_train)

    def _drop_id_label_cols(self, df, mlb_categories):
        return df.drop(
            list(mlb_categories) + self.id_cols_to_remove,
            axis=1,
        )

    def _remove_punctuation(self, text_column):
        free_text = "".join(
            [i for i in text_column if i not in string.punctuation]
        )
        return free_text

    def _get_model(self, model_name, hp):
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
        elif model_name == "XGB":
            return
            # return xgb.XGBClassifier(
            #     n_jobs=-3,
            #     n_estimators=hp[0],
            #     max_depth=hp[1],
            #     learning_rate=hp[2],
            # )
        elif model_name == "MNB":
            model = MultinomialNB(alpha=hp[0])
            return MultiOutputClassifier(model)
        elif model_name == "MLR":
            model = LogisticRegression(
                penalty=hp[0], C=hp[1], solver=hp[2], max_iter=hp[3]
            )
            return MultiOutputClassifier(model)
        else:
            logging.info(f"Model name {model_name} not exist")

    def _generate_model_id(self, train_model, model_name, cohort_id, hp_id):
        """Generate model id based on model name, cohort ID"""
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

    def _clean_for_model_id(self, word):
        """Clean word arguments for model id and concatenate together."""
        word = str(word).replace("-", "")
        return word.lower()

    def _save_trained_model(self, train_model, model_path, model_id, run_date):
        filename = (
            "_".join([model_id, run_date.strftime("%Y%m%d_%H%M%S%f")])
            + ".joblib"
        )
        dump(train_model, os.path.join(model_path, filename))

    def _build_hyperparameters_id(self, model_name, hp):
        if model_name == "DTC":
            return f"max_depth{hp[0]}"
        elif model_name == "RFR":
            return f"n_estimators{hp[0]}_max_depth{hp[1]}"
        elif model_name == "MNB":
            return f"alpha{hp[0]}"
        elif model_name == "MLR":
            return f"penalty{hp[0]}_C{hp[1]}"

    def _generate_model_metadata(
        self, model_id, model_set, run_date, labels, hp_id, engine
    ):
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
            logging.info("Inserting model information to database")
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
        cohort_id,
        model,
        X_train,
        Y_train,
        model_path,
        run_date,
        hyperparams,
        schema_type,
    ):
        return Parallel(n_jobs=-2, backend="threading")(
            delayed(self.execute_one_model)(
                cohort_id,
                model,
                X_train,
                Y_train,
                model_path,
                run_date,
                hp,
                schema_type,
            )
            for hp in hyperparams
        )

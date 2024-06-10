import csv
import logging
import os
from typing import List

import mlflow
import scipy.sparse as sp
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor
from src.features.build_features import BuildFeaturesRandomForest
from src.utils.utils import get_data

from config.model_settings import BuildFeaturesConfig, MatrixGeneratorConfig

# Matrix generator
# Input: label, features, time_splits
# Output: train_df = numpy array, valid_df = numpy arr

# for each text column of interest
# run feature generator
# add resulting object to a list
# matrix generator: merge everything in the list together to get train_df and valid_df

logging.basicConfig(level=logging.INFO)


class MatrixGenerator:
    def __init__(self, algorithm: str, id_column_list: List[str]) -> None:
        self.algorithm = algorithm
        self.id_column_list = id_column_list

    @classmethod
    def from_dataclass_config(
        cls,
        config: MatrixGeneratorConfig,
    ) -> "MatrixGenerator":
        return cls(
            algorithm=config.ALGORITHM, id_column_list=config.ID_COLUMN_LIST
        )

    def execute_train_valid_set(self):
        cohorts_query = """select distinct "locationId", "cohort", "cohort_type",
        "train_validation_set" from "cohorts";"""
        cohorts_df = get_data(cohorts_query)

        return cohorts_df.train_validation_set.unique()

    def execute(self, engine, train_valid_id, run_date):
        cohorts_query = """select distinct * from "cohorts";"""
        cohorts_df = get_data(cohorts_query)

        return self.execute_for_cohort(
            engine, train_valid_id, cohorts_df, run_date
        )

    def execute_for_cohort(
        self,
        engine,
        training_validation_id,
        cohorts_df,
        run_date,
    ):
        cohort_df = cohorts_df.loc[
            cohorts_df["train_validation_set"] == training_validation_id
        ]
        # load labels
        # labels_df = self._load_all_labels(cohort_df)

        if cohort_df is not None:
            logging.info(
                f"Generating features for Cohort {training_validation_id}"
            )
            (
                train_df,
                validation_df,
                feature_train_id,
                feature_valid_id,
                labels_train_df,
                labels_valid_df,
            ) = self.matrix_generator(
                engine,
                cohort_df,
            )

            print("matrix_generator", len(train_df), len(validation_df))

            logging.info(f"Rows in training features: {train_df.shape[0]}")
            logging.info(
                f"Rows in validation features: {validation_df.shape[0]}"
            )
            # convert back to merge
            # labels_valid_df = pd.merge(
            #     feature_valid_id,
            #     labels_df,
            #     left_on=["location_id"],
            #     right_on=["locationId"],
            #     how="inner",
            # )
            # labels_train_df = pd.merge(
            #     feature_train_id,
            #     labels_df,
            #     left_on=["location_id"],
            #     right_on=["locationId"],
            #     how="inner",
            # )
            # write as pickle
            self._write_labels_as_csv(
                labels_train_df,
                run_date,
                training_validation_id,
                "training",
            )
            self._write_labels_as_csv(
                labels_valid_df,
                run_date,
                training_validation_id,
                "validation",
            )

            logging.info(
                f"Rows in training labels: {labels_train_df.shape[0]}"
            )
            logging.info(
                f"Rows in validation labels: {labels_valid_df.shape[0]}"
            )
            return validation_df, train_df, labels_valid_df, labels_train_df
        else:
            logging.info("training or validation cohort must be assigned")

    def matrix_generator(self, engine, cohort_df):
        if self.algorithm == "RFR":
            config = BuildFeaturesConfig()

            (
                df_train,
                df_valid,
                feature_train_id,
                feature_valid_id,
                train_labels,
                validation_labels,
            ) = (
                self._get_feature_generator()
                .from_dataclass_config(config)
                .execute(engine, cohort_df)
            )

            return (
                df_train,
                df_valid,
                feature_train_id,
                feature_valid_id,
                train_labels,
                validation_labels,
            )

    def _add_csr(self, df, train_validation_set, cohort_type, run_date):
        csr_list = self._get_csr(train_validation_set, cohort_type, run_date)
        csr = self._concat_csr(df, csr_list)
        filename = "_".join(
            [
                str(train_validation_set),
                cohort_type,
                run_date.strftime("%Y%m%d_%H%M%S%f"),
            ]
        )

        dump(
            csr,
            os.path.join(
                self.features_path,
                filename + ".joblib",
            ),
        )

        return csr

    def _get_feature_generator(self) -> RandomForestRegressor:
        if self.algorithm == "RFR":
            return BuildFeaturesRandomForest
        else:
            raise ValueError(
                "The algorithm provided has no registered feature builder!"
            )

    def _get_csr(self, train_validation_set, cohort_type, run_date):
        filename = "_".join(
            [
                str(train_validation_set),
                cohort_type,
                run_date.strftime("%Y%m%d_%H%M%S%f"),
            ]
        )
        return [
            load(
                os.path.join(
                    self.text_features_path,
                    x + "_" + filename + ".joblib",
                )
            )
            for x in self.text_column_list
        ]

    def _concat_csr(self, X, csr_list):
        structured_csr = sp.csr_matrix(X.drop(self.id_column_list, axis=1))
        csr_list += [structured_csr]
        return sp.hstack(csr_list)

    def _load_all_labels(self, cohort_df):
        labels_df = cohort_df[
            [
                "locationId",
                "value",
                "cohort",
                "cohort_type",
                "train_validation_set",
            ]
        ]
        return labels_df

    def _write_labels_as_csv(
        self, y, run_date, training_validation_id, cohort_type
    ):
        filename = "_".join(
            [
                "labels",
                run_date.strftime("%Y%m%d_%H%M%S%f"),
                str(training_validation_id),
                cohort_type,
            ]
        )
        f = open(f"{filename}.csv", "w")

        with f:

            writer = csv.writer(f)

            for row in y:
                writer.writerow(row)

        mlflow.log_artifact(filename + ".csv")

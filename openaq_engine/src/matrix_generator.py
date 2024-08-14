import csv
import logging
import os
from typing import Any, List, Tuple

import mlflow
import scipy.sparse as sp
from joblib import dump, load

from config.model_settings import BuildFeaturesConfig, MatrixGeneratorConfig
from openaq_engine.src.features.build_features import BuildFeaturesRandomForest
from openaq_engine.src.utils.utils import get_data

logging.basicConfig(level=logging.INFO)


class MatrixGenerator:
    """
    Class to generate matrices for training and validation sets using a specified algorithm.

    :param algorithm: The algorithm used for generating features (e.g., 'RFR' for RandomForestRegressor).
    :type algorithm: str
    :param id_column_list: A list of column names to be treated as identifiers.
    :type id_column_list: list
    """

    def __init__(self, algorithm: str, id_column_list: List[str]) -> None:
        self.algorithm = algorithm
        self.id_column_list = id_column_list

    @classmethod
    def from_dataclass_config(
        cls, config: MatrixGeneratorConfig
    ) -> "MatrixGenerator":
        """
        Create a MatrixGenerator instance from a configuration dataclass.

        :param config: The configuration dataclass.
        :type config: MatrixGeneratorConfig
        :return: An instance of MatrixGenerator.
        :rtype: MatrixGenerator
        """
        return cls(
            algorithm=config.ALGORITHM, id_column_list=config.ID_COLUMN_LIST
        )

    def execute_train_valid_set(self, place: str) -> List[str]:
        """
        Execute a query to get unique train/validation sets for a specific location.

        :param place: The location for which the train/validation sets are to be retrieved.
        :type place: str
        :return: A list of unique train/validation sets.
        :rtype: list
        """
        cohorts_query = f"""select distinct "location", "cohort", "cohort_type",
        "train_validation_set" from "cohorts_local_{place}";"""
        cohorts_df = get_data(cohorts_query)

        return cohorts_df.train_validation_set.unique()

    def execute(
        self, engine: Any, train_valid_id: str, run_date: Any
    ) -> Tuple[Any, Any, Any, Any]:
        """
        Execute the matrix generation for a specific train/validation ID and date.

        :param engine: The database engine used for the operations.
        :type engine: Any
        :param train_valid_id: The train/validation ID for which to generate the matrix.
        :type train_valid_id: str
        :param run_date: The date of the run, used for file naming.
        :type run_date: Any
        :return: The validation dataframe, training dataframe, validation labels, and training labels.
        :rtype: tuple
        """
        cohorts_query = """select distinct * from "cohorts";"""
        cohorts_df = get_data(cohorts_query)

        return self.execute_for_cohort(
            engine, train_valid_id, cohorts_df, run_date
        )

    def execute_for_cohort(
        self,
        engine: Any,
        training_validation_id: str,
        cohorts_df: Any,
        run_date: Any,
    ) -> Tuple[Any, Any, Any, Any]:
        """
        Generate features and labels for a specific cohort.

        :param engine: The database engine used for the operations.
        :type engine: Any
        :param training_validation_id: The ID of the training/validation set.
        :type training_validation_id: str
        :param cohorts_df: The dataframe containing cohort information.
        :type cohorts_df: Any
        :param run_date: The date of the run, used for file naming.
        :type run_date: Any
        :return: The validation dataframe, training dataframe, validation labels, and training labels.
        :rtype: tuple
        """
        cohort_df = cohorts_df.loc[
            cohorts_df["train_validation_set"] == training_validation_id
        ]

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
            ) = self.matrix_generator(engine, cohort_df)

            logging.info(f"Rows in training features: {train_df.shape[0]}")
            logging.info(
                f"Rows in validation features: {validation_df.shape[0]}"
            )

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
            logging.info("Training or validation cohort must be assigned")

    def matrix_generator(
        self, engine: Any, cohort_df: Any
    ) -> Tuple[Any, Any, Any, Any, Any, Any]:
        """
        Generate the feature matrix for training and validation sets.

        :param engine: The database engine used for the operations.
        :type engine: Any
        :param cohort_df: The dataframe containing cohort information.
        :type cohort_df: Any
        :return: The training dataframe, validation dataframe, training feature IDs, validation feature IDs, training labels, and validation labels.
        :rtype: tuple
        """
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

    def _add_csr(
        self,
        df: Any,
        train_validation_set: str,
        cohort_type: str,
        run_date: Any,
    ) -> sp.csr_matrix:
        """
        Add a CSR (Compressed Sparse Row) matrix for the given dataframe.

        :param df: The dataframe to be converted to CSR.
        :type df: Any
        :param train_validation_set: The ID of the train/validation set.
        :type train_validation_set: str
        :param cohort_type: The type of cohort (e.g., 'training', 'validation').
        :type cohort_type: str
        :param run_date: The date of the run, used for file naming.
        :type run_date: Any
        :return: The CSR matrix.
        :rtype: sp.csr_matrix
        """
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

    def _get_feature_generator(self) -> Any:
        """
        Retrieve the appropriate feature generator based on the algorithm.

        :return: The feature generator class.
        :rtype: Any
        """
        if self.algorithm == "RFR":
            return BuildFeaturesRandomForest
        else:
            raise ValueError(
                "The algorithm provided has no registered feature builder!"
            )

    def _get_csr(
        self, train_validation_set: str, cohort_type: str, run_date: Any
    ) -> sp.csr_matrix:
        """
        Load a CSR matrix from a file.

        :param train_validation_set: The ID of the train/validation set.
        :type train_validation_set: str
        :param cohort_type: The type of cohort (e.g., 'training', 'validation').
        :type cohort_type: str
        :param run_date: The date of the run, used for file naming.
        :type run_date: Any
        :return: The loaded CSR matrix.
        :rtype: sp.csr_matrix
        """
        filename = "_".join(
            [
                str(train_validation_set),
                cohort_type,
                run_date.strftime("%Y%m%d_%H%M%S%f"),
            ]
        )
        file_path = os.path.join(filename + ".joblib")
        return load(file_path)

    def _concat_csr(
        self, X: Any, csr_list: List[sp.csr_matrix]
    ) -> sp.csr_matrix:
        """
        Concatenate multiple CSR matrices.

        :param X: The dataframe to be converted to CSR.
        :type X: Any
        :param csr_list: A list of CSR matrices to be concatenated.
        :type csr_list: list
        :return: The concatenated CSR matrix.
        :rtype: sp.csr_matrix
        """
        structured_csr = sp.csr_matrix(X.drop(self.id_column_list, axis=1))
        csr_list += [structured_csr]
        return sp.hstack(csr_list)

    def _load_all_labels(self, cohort_df: Any) -> Any:
        """
        Load all labels for the given cohort dataframe.

        :param cohort_df: The dataframe containing cohort information.
        :type cohort_df: Any
        :return: A dataframe containing the labels.
        :rtype: Any
        """
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
        self,
        y: Any,
        run_date: Any,
        training_validation_id: str,
        cohort_type: str,
    ) -> None:
        """
        Write labels to a CSV file and log the artifact with MLflow.

        :param y: The dataframe containing labels.
        :type y: Any
        :param run_date: The date of the run, used for file naming.
        :type run_date: Any
        :param training_validation_id: The ID of the train/validation set.
        :type training_validation_id: str
        :param cohort_type: The type of cohort (e.g., 'training', 'validation').
        :type cohort_type: str
        """
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

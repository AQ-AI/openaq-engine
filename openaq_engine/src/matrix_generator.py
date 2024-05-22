import csv
import logging
import os
from typing import List

import mlflow
import scipy.sparse as sp
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor
from src.features.build_features import BuildFeaturesRandomForest

from config.model_settings import (
    BuildFeaturesConfig,
    MatrixGeneratorConfig,
)

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

    def execute(self, engine, x, y, timestamp_hour):
        logging.info(
            f"Generating features for location ({x}, {y}) at {timestamp_hour}"
        )
        df = self.matrix_generator(engine, x, y, timestamp_hour)
        return df

    def matrix_generator(self, engine, x, y, timestamp_hour):
        if self.algorithm == "RFR":
            config = BuildFeaturesConfig()

            df = (
                self._get_feature_generator()
                .from_dataclass_config(config)
                .execute(engine, x, y, timestamp_hour)
            )

            return df

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
                    filename + ".joblib",
                )
            )
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
        print(y, run_date, training_validation_id, cohort_type)
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

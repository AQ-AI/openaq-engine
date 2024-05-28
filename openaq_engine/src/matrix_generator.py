import csv
import logging
import os
from typing import Dict, List

import mlflow
import pandas as pd
import scipy.sparse as sp
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor
from setup_environment import get_dbengine
from src.features.build_features import BuildFeaturesRandomForest
from src.utils.utils import get_data, write_to_db

from config.model_settings import BuildFeaturesConfig, MatrixGeneratorConfig

logging.basicConfig(level=logging.INFO)


class MatrixGenerator:
    def __init__(
        self, algorithm: str, id_column_list: List[str], satellite_config: dict
    ) -> None:
        self.algorithm = algorithm
        self.id_column_list = id_column_list
        self.satellite_config = satellite_config

    @classmethod
    def from_dataclass_config(
        cls,
        config: MatrixGeneratorConfig,
    ) -> "MatrixGenerator":
        return cls(
            algorithm=config.ALGORITHM,
            id_column_list=config.ID_COLUMN_LIST,
            satellite_config=config.SATELLITE_CONFIG,
        )

    def execute(self, engine, x, y, table_name):
        logging.info(f"Generating features for location ({x}, {y})")
        df = self.matrix_generator(engine, x, y, table_name)
        return df

    def matrix_generator(self, engine, x, y, table_name):
        if self.algorithm == "RFR":

            df = self._get_feature_generator(self.satellite_config).execute(
                engine, x, y, table_name
            )

            return df

    def _get_feature_generator(
        self, satellite_config: dict
    ) -> RandomForestRegressor:
        if self.algorithm == "RFR":
            config = BuildFeaturesConfig()
            return BuildFeaturesRandomForest.from_dataclass_config(
                satellite_config, config
            )
        else:
            raise ValueError(
                "The algorithm provided has no registered feature builder!"
            )

    def build_features(self, cohort_table):
        cohort_time_ranges = self.extract_time_ranges(cohort_table)
        cohort_data_dict = {}

        for tv_id, types in cohort_time_ranges.items():
            cohort_data_dict[tv_id] = {"training": [], "validation": []}

            locations_query = f"""
                SELECT DISTINCT x, y FROM "{cohort_table}"
                WHERE train_validation_set = {tv_id}
            """
            locations = get_data(locations_query)

            for c_type, time_ranges in types.items():
                print(f"Processing {c_type} data for TV set {tv_id}")
                for start_date, end_date in time_ranges:
                    print(f"Time range: {start_date} to {end_date}")
                    for _, loc in locations.iterrows():
                        x, y = loc["x"], loc["y"]
                        print(f"Processing location: ({x}, {y})")
                        sat_data = self.query_satellite_data(
                            tv_id, x, y, start_date, end_date, cohort_table
                        )
                        if not sat_data.empty:
                            cohort_data_dict[tv_id][c_type].append(sat_data)

                # Concatenate DataFrames if there are any
                if cohort_data_dict[tv_id][c_type]:
                    concatenated_df = pd.concat(
                        cohort_data_dict[tv_id][c_type]
                    )
                    table_name = f"{tv_id}_{c_type}"
                    print(
                        f"Writing to DB: {table_name} with {len(concatenated_df)} rows"
                    )
                    write_to_db(
                        concatenated_df,
                        get_dbengine(),
                        table_name,
                        "public",
                        "replace",
                    )
                else:
                    print(f"No data to write for {tv_id} {c_type}")

        return cohort_data_dict

    def extract_time_ranges(
        self, cohort_table: str
    ) -> Dict[int, Dict[str, List]]:
        query = f"""
            SELECT DISTINCT train_validation_set, cohort, cohort_type FROM "{cohort_table}"
        """
        cohort_data = get_data(query)
        cohort_time_ranges = {}

        for _, row in cohort_data.iterrows():
            tv_set, cohort, c_type = (
                row["train_validation_set"],
                row["cohort"],
                row["cohort_type"],
            )
            start_date, end_date = cohort.split("_")[1:]

            if tv_set not in cohort_time_ranges:
                cohort_time_ranges[tv_set] = {"training": [], "validation": []}

            cohort_time_ranges[tv_set][c_type].append((start_date, end_date))

        return cohort_time_ranges

    def query_satellite_data(
        self, tv_id, x, y, start_date, end_date, cohort_table
    ):
        # Query to get the cohort data
        cohort_query = f"""
            SELECT x, y, value, date_trunc('hour', "timestamp_utc"::timestamp) AS "datetime_hour"
            FROM "{cohort_table}"
            WHERE x = {x} AND y = {y}
            AND "timestamp_utc"::timestamp BETWEEN '{start_date}' AND '{end_date}'
            AND train_validation_set = {tv_id}
        """
        cohort_df = get_data(cohort_query)
        if cohort_df.empty:
            print(
                f"No cohort data found for location ({x}, {y}) and TV set {tv_id} between {start_date} and {end_date}"
            )
            return (
                pd.DataFrame()
            )  # Return an empty DataFrame if there's no cohort data

        cohort_df["datetime_hour"] = pd.to_datetime(
            cohort_df["datetime_hour"], utc=True
        )
        cohort_df["sensor_longitude"] = cohort_df["x"]
        cohort_df["sensor_latitude"] = cohort_df["y"]

        for satellite, config in self.satellite_config.items():
            table_name = satellite.replace("/", "_") + "_local_MN_new"
            columns = ", ".join([f'"{band}"' for band in config["bands"]])
            query = f"""
                SELECT sensor_longitude, sensor_latitude, date_trunc('hour', "datetime"::timestamp) AS "datetime_hour", {columns}
                FROM "{table_name}"
                WHERE sensor_longitude = {x} AND sensor_latitude = {y}
                AND "datetime"::timestamp BETWEEN '{start_date}' AND '{end_date}'
            """
            sat_df = get_data(query)
            frequency = config["frequency"]
            if not sat_df.empty:
                # Aggregate to avoid duplicate datetime_hour values
                sat_df["datetime_hour"] = pd.to_datetime(
                    sat_df["datetime_hour"], utc=True
                )
                sat_df = sat_df.groupby("datetime_hour").mean().reset_index()

                # Expand lower frequency data to match hourly intervals
                if frequency in ["monthly", "weekly"]:
                    sat_df = (
                        sat_df.set_index("datetime_hour")
                        .resample("h")
                        .ffill()
                        .reset_index()
                    )

                cohort_df = pd.merge(
                    cohort_df,
                    sat_df,
                    on=[
                        "sensor_longitude",
                        "sensor_latitude",
                        "datetime_hour",
                    ],
                    how="outer",
                    suffixes=("", f"_{satellite}"),
                )
            else:
                print(
                    f"No satellite data found for {satellite} at location ({x}, {y}) between {start_date} and {end_date}"
                )

        # Sorting by datetime and sensor location for better readability
        cohort_df.sort_values(
            by=["datetime_hour", "sensor_longitude", "sensor_latitude"],
            inplace=True,
        )

        return cohort_df

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

import json
from typing import Any, List, Union

import mlflow
import numpy as np
import pandas as pd
import yaml
from pydantic.json import pydantic_encoder


def parametrized(dec):
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)

        return repl

    return layer


def get_latest_successful_run_id(
    experiment_id: Union[int, str], filter_string: str = ""
) -> str:
    """
    Get the id of the latest successful run

    Parameters
    ----------
    experiment_id : int
        Id of the experiment
    filter_string : str, optional
        Filter query to apply to runs of the experiment, by default ""

    Returns
    -------
    str
        Id of the run

    Raises
    ------
    ValueError
        When the experiment does not have any successful runs
    """
    run_df = mlflow.search_runs(
        [str(experiment_id)], filter_string=filter_string
    )

    finished_df = run_df[run_df.status == "FINISHED"]

    if finished_df.empty:
        raise ValueError(
            f"No successful run was found for experiment id {experiment_id}"
        )

    return finished_df.iloc[0].run_id


def write_csv(df: pd.DataFrame, path: str, **kwargs: Any) -> None:
    """
    Write csv to provided path ensuring that the correct encoding and escape
    characters are applied.

    Needed when csv's have text with html tags in it and lists inside cells.
    """
    df.to_csv(
        path,
        index=False,
        na_rep="",
        sep=",",
        line_terminator="\n",
        encoding="utf-8",
        escapechar="\r",
        **kwargs,
    )


def write_dataclass(dclass: object, path: str) -> None:
    """
    Write a dataclass to the provided path as a json

    """
    with open(path, "w+") as f:
        f.write(
            json.dumps(
                dclass, indent=4, ensure_ascii=True, default=pydantic_encoder
            )
        )


def get_or_create_experiment(experiment_name: str) -> int:
    """
    Get experiment id by experiment name. Creates an experiment and returns its
    ID if no experiment by the name exists

    Parameters
    ----------
    experiment_name : str
        Name of the experiment

    Returns
    -------
    int
        ID of the experiment
    """
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if not experiment:
        experiment_id = mlflow.create_experiment(experiment_name)
    else:
        experiment_id = experiment.experiment_id

    return experiment_id


def get_categorical_feature_indices(df: pd.DataFrame) -> List[int]:
    return list(np.where(df.dtypes == "category")[0])


def yaml_provider(file_name, cmd_name):
    with open(file_name) as f:
        return yaml.safe_load(f)


def json_provider(file_path, cmd_name):
    with open(file_path) as config_data:
        return json.load(config_data)

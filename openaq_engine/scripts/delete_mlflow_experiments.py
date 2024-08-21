import argparse

import mlflow


def delete_experiments_with_filter(filter_string):
    # List all experiments with a filter
    experiments = mlflow.search_experiments(
        filter_string=f"name ILIKE '%{filter_string}%'"
    )

    # Delete each filtered experiment
    for exp in experiments:
        mlflow.delete_experiment(exp.experiment_id)
        print(f"Deleted experiment: {exp.name} (ID: {exp.experiment_id})")


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description="Delete MLflow experiments based on a filter string."
    )
    parser.add_argument(
        "filter_string",
        type=str,
        help="The string to filter experiment names.",
    )

    args = parser.parse_args()

    # Delete experiments with the provided filter string
    delete_experiments_with_filter(args.filter_string)

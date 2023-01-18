import datetime as dt
import logging
import os

import matplotlib.pyplot as plt
from src.utils.utils import get_data, write_to_db

from config.model_settings import ModelEvaluatorConfig, ModelVisualizerConfig


class ModelVisualizer:
    def __init__(
        self,
        plot,
        plot_metrics,
        plots_table_name,
        results_table_name,
    ) -> None:
        self.plot = plot
        self.plot_metrics = plot_metrics
        self.plots_table_name = plots_table_name
        self.results_table_name = results_table_name

    @classmethod
    def from_dataclass_config(
        cls, config: ModelVisualizerConfig, eval_config: ModelEvaluatorConfig
    ) -> "ModelVisualizer":
        """Imports data from the config class"""
        return cls(
            plot=config.PLOT,
            plot_metrics=config.PLOT_METRICS,
            plots_table_name=config.PLOTS_TABLE_NAME,
            results_table_name=config.RESULTS_TABLE_NAME,
        )

    def execute(
        self,
        path=None,
        all_models=True,
        last_run_model=None,
        run_date=None,
        model_ids=[],
        figsize=(10, 8),
    ):
        """
        Creates plot of average precision and recall for a model run and model ID.

        Arguments
        ---------
            path : str
                file path on server to save plots
            all_models : boolean
                generate plots for all models
            run_date : date
                specific run date
            last_run_model : str
                model id for returning the most recent run
            model_ids : list
                list of model ids, empty if all_models = True
            figsize : tuple
                tuple for figure size of plot
        """
        df = self.get_results(run_date=run_date, last_run_model=last_run_model)

        if all_models:
            model_ids = df["model_id"].unique()

        for id in model_ids:
            fig, ax = plt.subplots(1, 1, figsize=figsize)
            plot_info = {
                "linestyle": {"R2": "-", "MSE": "-.", "MAPE": "--"},
            }
            for metric in self.plot_metrics:
                logging.info(f"Generating plot for {id} and {metric}")

                if self.plot:
                    df_filt = self._filter_df(df, metric, id)
                    self._generate_plot(
                        ax,
                        plot_info,
                        df_filt,
                        metric,
                    )
            ax.set_ylabel("Metric", fontsize=16)
            ax.set_xlabel("Model ID", fontsize=16)
            ax.set_title("Models and their metrics", fontsize=18)
            ax.set_ylim((0, 1))
            ax.legend(fontsize=16)

            if path is not None:
                plot_path = self._plot_path(path, id, metric, df)
                fig.savefig(plot_path)

                # write to db
                self._generate_db_tbl(df, plot_path)

    def get_results(self, run_date=None, last_run_model=None):
        """Query the results data from the database for a specific run date and time"""
        if run_date is None and last_run_model is None:
            filter_query = """WHERE run_date = (SELECT MAX(run_date)
                        FROM {table})""".format(
                table=self.results_table_name
            )
        elif last_run_model is not None:
            filter_query = """WHERE model_id = '{last_run_model}' AND
                                run_date = (SELECT MAX(run_date)
                                FROM {table}
                                WHERE model_id = '{last_run_model}')""".format(
                table=self.results_table_name,
                last_run_model=last_run_model,
            )
        else:
            filter_query = f"""WHERE date_trunc('seconds', run_date) =
            TO_TIMESTAMP('{run_date}', 'YYYY-MM-DD HH24:MI:SS')"""

        df = get_data(
            """SELECT * FROM {table}
                        {filter_query}""".format(
                table=self.results_table_name,
                filter_query=filter_query,
            )
        )

        if df.shape[0] == 0:
            logging.warning(
                "Empty dataframe. Check run date or model_id if specified."
            )

        return df

    def _generate_plot(
        self,
        ax,
        plot_info,
        df,
        metric,
    ):
        """Generate plot of average precision and recall"""
        ax.plot(
            df["model_id"],
            df[metric],
            label=metric + " random forest",
            linestyle=plot_info["linestyle"][metric],
            linewidth=2,
        )
        return ax

    def _filter_df(self, df, metric, model_id):
        "Filter dataframe of results for summary method and model ID"
        if metric not in df["summary"].unique():
            logging.warning(f"Summary value {metric} not in dataframe")
            return df

        return df.loc[
            (df["summary"] == metric) & (df["model_id"] == model_id), :
        ]

    def _plot_path(self, path, model_id, metric, df):
        """Generate plot path fro model ID, metric, and run date"""
        run_date = df["run_date"].dt.strftime("%Y%m%d_%H%M%S%f").unique()[0]

        fname = "_".join([model_id, metric, run_date]) + ".png"
        return os.path.join(path, fname)

    def _generate_db_tbl(self, df, plot_path):
        """Write plot information and file path to the database"""
        plots_df = df.loc[
            :, ["model_id", "run_date", "summary"]
        ].drop_duplicates()
        plots_df["date_generated"] = dt.datetime.now()
        plots_df["plot_location"] = plot_path

        write_to_db(
            plots_df,
            self.engine,
            self.plots_table_name,
            self.plots_schema_name,
            "append",
        )

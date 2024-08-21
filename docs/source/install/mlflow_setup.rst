Mlflow Experiment Implementation
================================

Backend/Model Deployment
------------------------
Develop MLOps tooling wrapper for the current implementation of the global model to enable model experimentation.

**Status**: Complete

Methodology
-----------
The team used MLFlow to run each pipeline to gather, process, analyze, and validate the results of the registered machine learning model.

1. **Module Options**: Each module has a set of options to parameterize the CLI command using the specified arguments.
   
   .. code-block:: python

      from mlflows.cli.cohort_builder import cohort_builder_options

2. **Module Configuration**: Each module has a configuration file where default configs can be stored, as these vary less frequently.
   
   .. code-block:: python

      from config.model_settings import CohortBuilderConfig

3. **Flow Classes**: Each module has its own "flow class," which initializes the module with the configuration in a standard way.
   
   .. code-block:: python

      class CohortBuilderFlow:
          def __init__(self):
              self.config = CohortBuilderConfig()

          def execute(self):
              return CohortBuilder.from_dataclass_config(self.config)

4. **Command Execution**: Commands execute the modules and any dependent modules. Experiment IDs are the name of the module and the datetime of execution. Storage can be a hosted solution or a local path.
   
   .. code-block:: python

      @cohort_builder_options()
      @click.command("cohort-builder", help="Generate cohorts for time splits")
      def cohort_builder(country, source, pollutant, latest_date):
          experiment_id = mlflow.create_experiment(
              f"cohort_builder_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
          )

          with mlflow.start_run(experiment_id=experiment_id, nested=True):
              engine = get_dbengine()
              time_splitter = TimeSplitterFlow().execute()
              train_validation_dict = time_splitter.execute(
                  country, source, pollutant, latest_date
              )

              cohort_builder = CohortBuilderFlow().execute()
              cohort_builder.execute(
                  train_validation_dict, engine, country, source, pollutant
              )

5. **CLI Grouping**: Commands are added to the CLI group `openaq-engine`, grouping the modules into the package.
   
   .. code-block:: python

      @click.group("openaq-engine", help="Library to query openaq data")
      @click.pass_context
      def cli(ctx):
          ...

      cli.add_command(time_splitter)
      cli.add_command(cohort_builder)
      cli.add_command(feature_builder)
      cli.add_command(run_pipeline)

      if __name__ == "__main__":
          cli()

6. **MLFlow Tracking URI**: MLFlow initiates the tracking URI through environment variables. This should be set to your specified server or "localhost".
   
   .. code-block:: python

      mlflow.set_tracking_uri(
          os.getenv("MLFLOW_TRACKING_URI")
      )

Example: Extending the Library
------------------------------
If a user wants to extend the library and write their own class, they can follow these steps:

1. **Create a Configuration Class**: Define the configuration for the new module in `config.model_settings`.
   
   .. code-block:: python

      class CustomModuleConfig:
          PARAM1 = "default_value"
          PARAM2 = 10

2. **Create a Flow Class**: Define the flow class for the new module.
   
   .. code-block:: python

      class CustomModuleFlow:
          def __init__(self):
              self.config = CustomModuleConfig()

          def execute(self):
              # Implement the execution logic
              pass

3. **Define CLI Command**: Create a CLI command for the new module.
   
   .. code-block:: python

      import click
      from mlflows.cli.custom_module import custom_module_options
      from config.model_settings import CustomModuleConfig

      @custom_module_options()
      @click.command("custom-module", help="Description of custom module")
      def custom_module(param1, param2):
          experiment_id = mlflow.create_experiment(
              f"custom_module_{str(datetime.now())}", os.getenv("MLFLOW_S3_BUCKET")
          )

          with mlflow.start_run(experiment_id=experiment_id, nested=True):
              custom_module = CustomModuleFlow().execute()
              custom_module.execute(param1, param2)

4. **Add Command to CLI Group**: Add the new command to the CLI group.
   
   .. code-block:: python

      cli.add_command(custom_module)

5. **Run the CLI**: Execute the CLI with the new command.
   
   .. code-block:: sh

      python main.py custom-module --param1 value1 --param2 value2

This approach allows users to extend the library easily and integrate new modules into the existing MLFlow experiment framework.

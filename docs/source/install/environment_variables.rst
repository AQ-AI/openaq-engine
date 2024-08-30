Environment Variables
=====================

This document describes the environment variables used in the project and how to obtain their values.

Environment Variables
---------------------

The following environment variables are required for the project:

- `DB_NAME_OPENAQ`: Name of the OpenAQ database.
- `S3_OUTPUT_OPENAQ`: Output path in the S3 bucket for OpenAQ.
- `S3_BUCKET_OPENAQ`: Name of the S3 bucket for OpenAQ.
- `AWS_PROFILE`: AWS CLI profile to use.
- `AWS_USER`: AWS user name.
- `AWS_ACCESS_KEY`: AWS access key ID.
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key.
- `PGDATABASE`: PostgreSQL database name.
- `PGUSER`: PostgreSQL user name.
- `PGPASSWORD`: PostgreSQL password.
- `PGHOST`: PostgreSQL host.
- `EE_API_KEY`: API key for Earth Engine.
- `MLFLOW_S3_BUCKET`: S3 bucket for MLflow.
- `MLFLOW_TRACKING_URI`: Tracking URI for MLflow.
- `PYTHONPATH=$(pwd)`: Python path to the current working directory.
- `TEST_PGDATABASE`: PostgreSQL database name for testing.
- `TEST_PGUSER`: PostgreSQL user name for testing.
- `TEST_PGPASSWORD`: PostgreSQL password for testing.
- `TEST_PGHOST`: PostgreSQL host for testing.

Obtaining Environment Variables
-------------------------------

The environment variables can be obtained or set up as follows:

Database Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- `DB_NAME_OPENAQ`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGHOST`, `TEST_PGDATABASE`, `TEST_PGUSER`, `TEST_PGPASSWORD`, `TEST_PGHOST`:
  - These variables are related to your PostgreSQL database configuration. You should set them according to your database setup.

AWS Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- `S3_OUTPUT_OPENAQ`, `S3_BUCKET_OPENAQ`, `AWS_PROFILE`, `AWS_USER`, `AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `MLFLOW_S3_BUCKET`:
- These variables are related to your AWS configuration. You can obtain the access keys and bucket names from the AWS Management Console.
- `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` can be generated from the AWS IAM service. Ensure you have the necessary permissions to access the required resources.
- `AWS_PROFILE` is the name of the AWS CLI profile you are using, which is configured in your AWS credentials file (usually located at `~/.aws/credentials`).

Earth Engine API Key
^^^^^^^^^^^^^^^^^^^^

- `EE_API_KEY`:
- This is the API key for Google Earth Engine. You can generate an API key from the Google Earth Engine Developers Console.

MLflow Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- `MLFLOW_S3_BUCKET`, `MLFLOW_TRACKING_URI`:
- These variables are used for configuring MLflow with an S3 bucket and a tracking URI. You should set these according to your MLflow setup.

Python Path
^^^^^^^^^^^

- `PYTHONPATH=$(pwd)`:
- This sets the Python path to the current working directory. It ensures that the Python interpreter can locate the modules in your project.

Setting Environment Variables
-----------------------------

To set these environment variables, you can add them to your shell configuration file (e.g., `.bashrc`, `.zshrc`) or export them directly in your terminal session:

.. code-block:: bash

   export DB_NAME_OPENAQ=your_openaq_db_name
   export S3_OUTPUT_OPENAQ=your_s3_output_path
   export S3_BUCKET_OPENAQ=your_s3_bucket_name
   export AWS_PROFILE=your_aws_profile
   export AWS_USER=your_aws_user_name
   export AWS_ACCESS_KEY=your_aws_access_key
   export AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
   export PGDATABASE=your_pg_database_name
   export PGUSER=your_pg_user_name
   export PGPASSWORD=your_pg_password
   export PGHOST=your_pg_host
   export EE_API_KEY=your_ee_api_key
   export MLFLOW_S3_BUCKET=your_mlflow_s3_bucket
   export MLFLOW_TRACKING_URI=your_mlflow_tracking_uri
   export PYTHONPATH=$(pwd)
   export TEST_PGDATABASE=your_test_pg_database_name
   export TEST_PGUSER=your_test_pg_user_name
   export TEST_PGPASSWORD=your_test_pg_password
   export TEST_PGHOST=your_test_pg_host

Replace the placeholder values with the actual values for your environment.

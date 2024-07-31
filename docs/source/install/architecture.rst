AQAI Pipeline
=============

Overview
--------

The AQAI Pipeline is a comprehensive data processing and machine learning system designed to handle various data sources, process them, and generate predictive models. The pipeline consists of several key components:

1. Data Sources
2. AQAI.DB
3. AQAI.FLOW
4. AQAI.Board
5. VPC

Data Sources
------------

The pipeline ingests data from multiple sources:

- Global, historic data (PM2.5): Sourced from OpenAQ
- Global, current data: Source not specified in the diagram
- Local data:
  - raw .xlsx files
  - raw .xls files
  - raw .csv files

AQAI.DB
-------

This component handles data storage and processing:

- Ingests raw data from various file formats
- Stores data in raw psql tables
- Processes the raw data into processed psql tables
- Creates modelling psql tables for further analysis

AQAI.FLOW
---------

AQAI.FLOW is the core processing and machine learning component:

- Utilizes MLflow for pipeline management
- Includes a Pipeline Runner with the following stages:
  1. Time series
  2. Dataset Creator
  3. Feature Generator
  4. Matrix Generator
  5. Model Trainer
  6. Model Evaluator
- Interacts with a central Database for data storage and retrieval
- Performs score prediction and model evaluation

AQAI.Board
----------

This component serves as a dashboard or monitoring system:

- Utilizes Grafana for visualization
- Incorporates InfluxDB for time-series data storage

VPC (Virtual Private Cloud)
---------------------------

A separate AQAI.Board instance runs within a VPC:

- Includes ModelServing capabilities
- Stores Artifacts, likely for model deployment and serving

Integration
-----------

The components are integrated as follows:

- Data flows from various sources into AQAI.DB
- AQAI.DB feeds processed and modelling data into AQAI.FLOW
- AQAI.FLOW interacts with its internal database and produces model artifacts
- Model artifacts are stored in the VPC for serving
- AQAI.Board instances provide monitoring and visualization capabilities

This pipeline allows for efficient data ingestion, processing, model training, evaluation, and deployment in a cohesive system.
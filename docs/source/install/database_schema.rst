AQAI Database Schema
====================

This document provides an overview of the PostgreSQL table schemas used in the AQAI system. Each schema is described in detail with example tables.

Cohorts Table
-------------

The `cohorts` table contains all raw data for the specified pollutant in the location and time range required.

**Schema:**

- `locationId` (integer): Unique identifier for the location.
- `location` (text): Name of the location.
- `parameter` (text): Type of pollutant.
- `value` (float): Measured value of the pollutant.
- `date` (json): Date and time of the measurement in UTC and local time.
- `unit` (text): Unit of measurement.
- `coordinates` (json): Coordinates of the measurement location.
- `country` (text): Country of the measurement location.
- `city` (text): City of the measurement location.
- `isMobile` (boolean): Indicates if the measurement was taken from a mobile source.
- `isAnalysis` (boolean): Indicates if the measurement is an analysis.
- `entity` (text): Entity responsible for the measurement.
- `sensorType` (text): Type of sensor used.
- `train_validation_set` (text): Indicates if the data is for training or validation.
- `cohort` (text): Cohort identifier.
- `cohort_type` (text): Type of cohort.
- `timestamp_utc` (timestamp): UTC timestamp of the measurement.
- `timestamp_local` (timestamp): Local timestamp of the measurement.
- `y` (float): Latitude of the measurement location.
- `x` (float): Longitude of the measurement location.

**Example:**

.. list-table::
   :header-rows: 1

   * - locationId
     - location
     - parameter
     - value
     - date
     - unit
     - coordinates
     - country
     - city
     - isMobile
     - isAnalysis
     - entity
     - sensorType
     - train_validation_set
     - cohort
     - cohort_type
     - timestamp_utc
     - timestamp_local
     - y
     - x
   * - 23
     - Amgalan
     - pm25
     - 181
     - {"utc": "2019-03-13T22:30:00+00:00", "local": "2019-03-14T06:30:00+08:00"}
     - µg/m³
     - {"latitude": 47.913429, "longitude": 106.99790699999998}
     - MN
     - NULL
     - f
     - NULL
     - Governmental Organization
     - reference grade
     - 0
     - 0_2015-09-01T00:00:00Z_2023-11-30T00:00:00Z
     - training
     - 2019-03-13T22:30:00.000000Z
     - 2019-03-14T06:30:00+0800
     - 47.913429
     - 106.997907

Features Table
--------------

The `features` table gathers all satellite variables spatially and temporally co-located with the pollutant observation.

**Schema:**

- `sensor_datetime` (timestamp): Date and time of the sensor reading.
- `sensor_longitude` (float): Longitude of the sensor reading.
- `sensor_latitude` (float): Latitude of the sensor reading.
- `location_id` (text): Identifier for the location.
- `Optical_Depth_047` (float): Optical depth at 0.47 microns.
- `B4` (float): Band 4 reflectance.
- `B3` (float): Band 3 reflectance.
- `B2` (float): Band 2 reflectance.
- `avg_rad` (float): Average radiance.
- `temperature_2m_above_ground` (float): Temperature 2 meters above ground.
- `relative_humidity_2m_above_ground` (float): Relative humidity 2 meters above ground.
- `total_precipitation_surface` (float): Total precipitation at surface level.
- `total_cloud_cover_entire_atmosphere` (float): Total cloud cover for the entire atmosphere.
- `u_component_of_wind_10m_above_ground` (float): U component of wind at 10 meters above ground.
- `v_component_of_wind_10m_above_ground` (float): V component of wind at 10 meters above ground.
- `discrete_classification` (integer): Discrete classification value.

**Example:**

.. list-table::
   :header-rows: 1

   * - sensor_datetime
     - sensor_longitude
     - sensor_latitude
     - location_id
     - Optical_Depth_047
     - B4
     - B3
     - B2
     - avg_rad
     - temperature_2m_above_ground
     - relative_humidity_2m_above_ground
     - total_precipitation_surface
     - total_cloud_cover_entire_atmosphere
     - u_component_of_wind_10m_above_ground
     - v_component_of_wind_10m_above_ground
     - discrete_classification
   * - 2019-03-11 20:30:00
     - 106.997907
     - 47.913429
     - Amgalan
     - 117.976936723832
     - NULL
     - NULL
     - NULL
     - 25.4099998474121
     - -0.12999572753904
     - 33.2400001525879
     - 0.0300000004470348
     - 99.8
     - -0.547999973595142
     - -0.589999985694885
     - 30

Model Metadata Table
--------------------

The `model_metadata` table stores all outputs of the machine learning pipeline. It includes transactions and codes data from the processed schema joined on the hash of the diagnosis note. This serves as the complete training and validation set, which is then split into temporal splits for cross validation.

**Schema:**

- `model_id` (text): Unique identifier for the model.
- `model_set` (text): Set identifier for the model.
- `features` (json): Features used in the model.
- `labels` (json): Labels used in the model.
- `hyperparameters` (text): Hyperparameters used for the model.
- `run_date` (timestamp): Date and time when the model was run.
- `timestamp` (timestamp): Timestamp of the record.

**Example:**

.. list-table::
   :header-rows: 1

   * - model_id
     - model_set
     - features
     - labels
     - hyperparameters
     - run_date
     - timestamp
   * - randomforestregressor_n_estimators500_max_depth10_8
     - randomforestregressor_n_estimators500_max_depth10
     - {Optical_Depth_047, SR_B4, SR_B3, SR_B2, avg_rad, temperature_2m_above_ground, relative_humidity_2m_above_ground, precipitable_water_entire_atmosphere, total_cloud_cover_entire_atmosphere, u_component_of_wind_10m_above_ground, v_component_of_wind_10m_above_ground}
     - {value}
     - n_estimators500_max_depth10
     - 2024-05-31 14:48:41.217907
     - 2024-05-31 14:48:41.217907

Results Table
-------------

The `results` table is used for model comparison and selection using Mean Squared Error (MSE) and Mean Absolute Percentage Error (MAPE).

**Schema:**

- `model_id` (text): Unique identifier for the model.
- `run_date` (timestamp): Date and time when the model was run.
- `cohort` (text): Cohort identifier.
- `mse` (numeric): Mean Squared Error of the model.
- `mape` (numeric): Mean Absolute Percentage Error of the model.

**Example:**

.. list-table::
   :header-rows: 1

   * - model_id
     - run_date
     - cohort
     - mse
     - mape
   * - randomforestregressor_n_estimators500_max_depth10_5
     - NULL
     - 5
     - 2069.5940127034537
     - 1846990659123641.2

from datetime import datetime

import mlflow.pyfunc
import pandas as pd

# Load the pre-trained model
logged_model = "runs:/18197340a0aa46b9bd2610d5582780ae/randomforestregressor_n_estimators500_max_depth10_min_samples_split10_min_samples_leaf2_max_featuressqrt_3"
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Read the filled satellite data CSV file
file_path = "filled_satellite_data.csv"
data = pd.read_csv(file_path)

# Function to parse the datetime column to datetime objects
data["current_timestamp"] = pd.to_datetime(data["current_timestamp"])

data["avg_rad"] = 0
data["avg_rad_time_diff"] = 0
# Additional features needed
CORE_FEATURES = [
    "current_timestamp",
    "latitude",
    "longitude",
    "Optical_Depth_047",
    "Optical_Depth_047_time_diff",
    "SR_B4",
    "SR_B4_time_diff",
    "SR_B3",
    # "SR_B3_time_diff",
    "SR_B2",
    # "SR_B2_time_diff",
    "avg_rad",
    "avg_rad_time_diff",
    "temperature_2m_above_ground",
    "temperature_2m_above_ground_time_diff",
    "relative_humidity_2m_above_ground",
    # "relative_humidity_2m_above_ground_time_diff",
    "precipitable_water_entire_atmosphere",
    # "precipitable_water_entire_atmosphere_time_diff",
    "u_component_of_wind_10m_above_ground",
    # "u_component_of_wind_10m_above_ground_time_diff",
    "v_component_of_wind_10m_above_ground",
    # "v_component_of_wind_10m_above_ground_time_diff"
]

# Filter the data to keep only the necessary features for prediction
prediction_data = data[CORE_FEATURES]

# Prepare the data for predictions
# Assuming that the timestamp is needed as a float for predictions, you can convert it
prediction_data["timestamp_as_float"] = prediction_data[
    "current_timestamp"
].apply(lambda x: x.timestamp())

# Re-order columns to match model input, if necessary
prediction_data = prediction_data[
    ["timestamp_as_float", "latitude", "longitude"]
    + [
        col
        for col in prediction_data.columns
        if col not in ["timestamp_as_float", "latitude", "longitude"]
    ]
]

# Drop the current_timestamp column as it's not needed for predictions and can cause dtype issues
prediction_data = prediction_data.drop(columns=["current_timestamp"])

# Fill missing values with the last valid observation
prediction_data_ffill = prediction_data.ffill()

# List of _time_diff columns to adjust
time_diff_columns = [
    col for col in prediction_data_ffill.columns if "time_diff" in col
]

# Adjust the _time_diff columns
for col in time_diff_columns:
    mask = (
        prediction_data[col].isna() & prediction_data_ffill[col].notna()
    )  # Identify where forward filling occurred
    prediction_data_ffill.loc[
        mask, col
    ] += 1  # Increment the _time_diff values by 1

# Ensure all columns are of numeric type
prediction_data_ffill = prediction_data_ffill.apply(pd.to_numeric)

# Make predictions using the loaded model
predictions = loaded_model.predict(prediction_data_ffill)

# Add predictions to the DataFrame
prediction_data_ffill["predictions"] = predictions
prediction_data_ffill["datetime"] = prediction_data_ffill[
    "timestamp_as_float"
].apply(lambda x: datetime.fromtimestamp(x))

# Save the predictions to a new CSV file
output_file_path = "predictions_filled_all_satellite_data.csv"
prediction_data_ffill.to_csv(output_file_path, index=False)

print(f"Predictions saved to {output_file_path}")

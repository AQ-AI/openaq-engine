import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import mlflow.pyfunc
import ee
import requests

# Initialize the Earth Engine module.
path_to_private_key = "unicef-367711-a4ac0921e063.json"
service_account = "earth-engine@unicef-367711.iam.gserviceaccount.com"
credentials = ee.ServiceAccountCredentials(
    service_account, path_to_private_key
)
ee.Initialize(credentials)

# Define the Ulaanbaatar polygon
ulaanbaatar_geometry = ee.Geometry.Polygon(
    [
        [
            [106.69167, 47.82976],
            [106.69167, 47.99405],
            [107.10423, 47.99405],
            [107.10423, 47.82976],
            [106.69167, 47.82976],
        ]
    ]
)

# Get the current date
endDate = ee.Date(datetime.utcnow())


# Get today's date
today = datetime.now()

# Get the first of January of the current year
first_of_january = datetime(today.year, 1, 1)

# Calculate the number of days between today and the first of January
days_difference = (today - first_of_january).days + 7

# Define the satellite configuration
SATELLATE_CONFIG = {
    "MODIS/061/MCD19A2_GRANULES": {
        "bands": ["Optical_Depth_047"],
        "resolution": 1000,
        "frequency": 7,
    },
    "LANDSAT/LC08/C02/T1_L2": {
        "bands": ["SR_B4", "SR_B3", "SR_B2"],
        "resolution": 30,
        "frequency": 30,
    },
    "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG": {
        "bands": ["avg_rad"],
        "resolution": 463.83,
        "frequency": days_difference,
    },
    "NOAA/GFS0P25": {
        "bands": [
            "temperature_2m_above_ground",
            "relative_humidity_2m_above_ground",
            "precipitable_water_entire_atmosphere",
            "u_component_of_wind_10m_above_ground",
            "v_component_of_wind_10m_above_ground",
        ],
        "resolution": 27830,
        "frequency": 7,
        "forecast_hour": 1,  # Specify the forecast hour you are interested in
    },
}

# Create a folder to save the images
os.makedirs("EarthEngineImages", exist_ok=True)


# Function to fetch and download images
def fetch_and_download_images(satellite_key, config):
    startDate = endDate.advance(-config["frequency"], "day")
    collection = (
        ee.ImageCollection(satellite_key)
        .filterDate(startDate, endDate)
        .filterBounds(ulaanbaatar_geometry)
    )

    if satellite_key == "NOAA/GFS0P25":
        forecast_hour = config.get("forecast_hour", 1)
        collection = collection.filter(
            ee.Filter.eq("forecast_hours", forecast_hour)
        )

    collection = collection.sort("system:time_start", False)

    # Check if the collection is empty
    if collection.size().getInfo() == 0:
        print(
            f"No images found for {satellite_key} in the specified date range."
        )
        return {}

    satellite_dict = {}

    def process_image(image):
        imageDate = (
            ee.Date(image.get("system:time_start"))
            .format("YYYY-MM-dd_HH-mm-ss")
            .getInfo()
        )
        availableBands = image.bandNames().getInfo()
        selectedBands = [
            band for band in config["bands"] if band in availableBands
        ]

        if selectedBands:
            fileName = satellite_key.replace("/", "_") + "_" + imageDate
            download_url = image.select(selectedBands).getDownloadURL(
                {"scale": config["resolution"], "region": ulaanbaatar_geometry}
            )

            # Download the image
            response = requests.get(download_url, stream=True)
            if response.status_code == 200:
                dest_path = f"EarthEngineImages/{fileName}.zip"
                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                # Check the size of the downloaded file
                if (
                    os.path.getsize(dest_path) > 844
                ):  # Set a threshold size (e.g., 1 KB)
                    print(f"Saved image to {dest_path}")
                    if satellite_key not in satellite_dict:
                        satellite_dict[satellite_key] = {}
                    satellite_dict[satellite_key][imageDate] = image.select(
                        selectedBands
                    )
                else:
                    print(
                        f"Skipping small or invalid image for {fileName} with size {os.path.getsize(dest_path)} bytes"
                    )
                    os.remove(dest_path)
            else:
                print(
                    f"Failed to download image for {fileName}, status code {response.status_code}"
                )
        else:
            print(f"No matching bands found for {satellite_key}")

    images = collection.toList(collection.size())
    for i in range(images.size().getInfo()):
        image = ee.Image(images.get(i))
        process_image(image)

    return satellite_dict


all_satellite_data = {}

# Iterate over the satellite configuration and fetch/save images
for satellite in SATELLATE_CONFIG:
    satellite_dict = fetch_and_download_images(
        satellite, SATELLATE_CONFIG[satellite]
    )
    if satellite_dict:
        all_satellite_data.update(satellite_dict)

print("Download tasks have been completed.")
print(all_satellite_data)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    filename="satellite_processing_all.log",
    filemode="w",
    format="%(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

# Read the uploaded CSV file
file_path = "data/centroid_data.csv"
data = pd.read_csv(file_path)


# Simplified function to parse the .geo column and extract coordinates
def parse_geo(geo_str):
    geo_dict = json.loads(geo_str)
    coordinates = geo_dict["coordinates"]
    return tuple(coordinates)


# Extract locations from the .geo column
locations = data[".geo"].apply(parse_geo)

# Verify coordinates format
logger.info("Sample coordinates from locations:")
for i, coord in enumerate(locations):
    if i < 5:  # Print first 5 coordinates for verification
        logger.info(coord)


def fetch_pixel_values_from_dict(
    satellite_data, coordinates, hourly_timestamp, most_recent_data
):
    closest_time_diff = float("inf")
    closest_pixel_values = {}

    for satellite_key, date_images in satellite_data.items():
        for date, image in date_images.items():
            image_time = datetime.strptime(date, "%Y-%m-%d_%H-%M-%S")
            if image_time <= hourly_timestamp:
                time_diff_hours = (
                    hourly_timestamp - image_time
                ).total_seconds() / 3600
                if time_diff_hours < closest_time_diff:
                    closest_time_diff = time_diff_hours
                    closest_pixel_values = {}
                    for band in image.bandNames().getInfo():
                        value = (
                            image.reduceRegion(
                                reducer=ee.Reducer.first(),
                                geometry=ee.Geometry.Point(coordinates),
                                scale=SATELLATE_CONFIG[satellite_key][
                                    "resolution"
                                ],
                            )
                            .get(band)
                            .getInfo()
                        )

                        if value is not None:  # Check if value is not None
                            closest_pixel_values[band] = value
                            closest_pixel_values[
                                f"{band}_time_diff"
                            ] = time_diff_hours

    # If no values found, use most recent data
    for satellite_key in most_recent_data:
        if not closest_pixel_values:
            for band in most_recent_data[satellite_key]["bands"]:
                value = most_recent_data[satellite_key]["values"].get(band)
                if value is not None:
                    closest_pixel_values[band] = value
                    closest_pixel_values[f"{band}_time_diff"] = (
                        hourly_timestamp
                        - most_recent_data[satellite_key]["timestamp"]
                    ).total_seconds() / 3600

    return closest_pixel_values


# Store the most recent data for each satellite
most_recent_data = {}
for satellite_key, date_images in all_satellite_data.items():
    most_recent_image_date = max(date_images.keys())
    most_recent_image = date_images[most_recent_image_date]
    most_recent_data[satellite_key] = {
        "timestamp": datetime.strptime(
            most_recent_image_date, "%Y-%m-%d_%H-%M-%S"
        ),
        "bands": most_recent_image.bandNames().getInfo(),
        "values": {
            band: most_recent_image.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=ulaanbaatar_geometry,
                scale=SATELLATE_CONFIG[satellite_key]["resolution"],
            )
            .get(band)
            .getInfo()
            for band in most_recent_image.bandNames().getInfo()
        },
    }


# Use joblib to parallelize the fetching process with throttling for each location
all_dfs = []
end_date = datetime.utcnow()
for coordinates in locations:
    results = []
    hourly_timestamps = [end_date - timedelta(hours=i) for i in range(7 * 24)]
    logger.info(f"coordinates: {coordinates}")
    for hourly_ts in hourly_timestamps:
        logger.info(f"hourly_ts: {hourly_ts}")
        pixel_values = fetch_pixel_values_from_dict(
            all_satellite_data, coordinates, hourly_ts, most_recent_data
        )
        if pixel_values:  # Check if we got valid pixel values
            results.append(
                {
                    "latitude": coordinates[1],
                    "longitude": coordinates[0],
                    "current_timestamp": hourly_ts,
                    **pixel_values,
                }
            )

    # Convert the results to a DataFrame and add it to the list
    if results:
        df = pd.DataFrame(results)
        all_dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
final_df = pd.concat(all_dfs, ignore_index=True)

# Save the final DataFrame to a CSV file
final_df.to_csv("satellite_data_7_day_images.csv", index=False)

logger.info("Data saved to satellite_data_7_day_images.csv")

# Forward-fill the missing values
data_filled = final_df.ffill()

# Save the updated DataFrame to a new CSV file
output_file_path = "filled_satellite_data.csv"
data_filled.to_csv(output_file_path, index=False)

print(f"Filled data saved to {output_file_path}")

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

# Save the predictions to a new CSV file
output_file_path = "predictions_filled_satellite_data.csv"
prediction_data_ffill.to_csv(output_file_path, index=False)

print(f"Predictions saved to {output_file_path}")

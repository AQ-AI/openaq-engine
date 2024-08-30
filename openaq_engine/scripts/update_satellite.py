import datetime
import json
import time

import ee
import pandas as pd
from joblib import Parallel, delayed


# Initialize the Earth Engine module
def initialize_ee():
    try:
        # Path to the service account key file
        path_to_private_key = "unicef-367711-a4ac0921e063.json"
        service_account = "earth-engine@unicef-367711.iam.gserviceaccount.com"
        credentials = ee.ServiceAccountCredentials(
            service_account, path_to_private_key
        )
        ee.Initialize(credentials)
        print("Earth Engine initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Earth Engine: {e}")
        raise


# Define the satellite configuration
SATELLITE_CONFIG = {
    "MODIS/061/MCD19A2_GRANULES": {
        "bands": ["Optical_Depth_047"],
        "resolution": 1000,
        "frequency": "daily",
    },
    "LANDSAT/LC08/C02/T1_L2": {
        "bands": ["SR_B4", "SR_B3", "SR_B2"],
        "resolution": 30,
        "frequency": "weekly",
    },
    "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG": {
        "bands": ["avg_rad"],
        "resolution": 463.83,
        "frequency": "monthly",
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
        "frequency": "daily",
    },
}

# Get the current date and the date 7 days ago
end_date = datetime.datetime.utcnow()
start_date = end_date - datetime.timedelta(days=7)


# Function to fetch the most recent image and pixel values
def fetch_recent_image_and_pixels(satellite_key, config, coordinates):
    initialize_ee()
    point_geometry = ee.Geometry.Point(coordinates)

    collection = (
        ee.ImageCollection(satellite_key)
        .filterDate(start_date.isoformat(), end_date.isoformat())
        .filterBounds(point_geometry)
        .sort("system:time_start", False)
    )
    image = collection.first()
    image_info = image.getInfo()

    if not image_info:
        return [], [], [], None, satellite_key, config["bands"]

    timestamp = ee.Date(image.get("system:time_start")).getInfo()["value"]
    timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp / 1000)

    pixel_values = []
    latitudes = []
    longitudes = []

    for band in config["bands"]:
        try:
            reduced = (
                image.reduceRegion(
                    reducer=ee.Reducer.toList(),
                    geometry=point_geometry,
                    scale=config["resolution"],
                    maxPixels=10000000,  # Increase maxPixels to handle large regions
                    bestEffort=True,
                )
                .get(band)
                .getInfo()
            )

            if reduced:
                pixel_values.extend(reduced)
                latitudes.extend([coordinates[1]] * len(reduced))
                longitudes.extend([coordinates[0]] * len(reduced))
            else:
                print(
                    f"No valid data for band {band} in {satellite_key} image"
                )
                pixel_values.append(None)
                latitudes.append(coordinates[1])
                longitudes.append(coordinates[0])
        except Exception as e:
            print(f"Error processing band {band} for {satellite_key}: {e}")
            pixel_values.append(None)
            latitudes.append(coordinates[1])
            longitudes.append(coordinates[0])

    return (
        pixel_values,
        latitudes,
        longitudes,
        timestamp_dt,
        satellite_key,
        config["bands"],
    )


# Read the uploaded CSV file
file_path = "data/centroid_data.csv"
data = pd.read_csv(file_path)


# Simplified function to parse the .geo column and extract coordinates
def parse_geo(geo_str):
    geo_dict = json.loads(geo_str)
    coordinates = geo_dict["coordinates"]
    return coordinates


# Extract locations from the .geo column
locations = data[".geo"].apply(parse_geo)


# Wrapper function to add delay between requests
def fetch_with_delay(satellite, config, coordinates, delay=1):
    result = fetch_recent_image_and_pixels(satellite, config, coordinates)
    time.sleep(delay)  # Add a delay between requests to avoid exceeding quotas
    return result


# Helper function to pad lists to the same length
def pad_lists(data_dict):
    max_length = max(len(lst) for lst in data_dict.values())
    for key in data_dict:
        if len(data_dict[key]) < max_length:
            data_dict[key].extend([None] * (max_length - len(data_dict[key])))
    return data_dict


# Use joblib to parallelize the fetching process with throttling for each location
all_dfs = []
for coordinates in locations:
    results = Parallel(n_jobs=-1)(
        delayed(fetch_with_delay)(satellite, config, coordinates)
        for satellite, config in SATELLITE_CONFIG.items()
    )

    # Create a DataFrame to store the results for this location
    data = {"latitude": [], "longitude": [], "current_timestamp": []}

    for (
        pixel_values,
        latitudes,
        longitudes,
        timestamp_dt,
        satellite_key,
        bands,
    ) in results:
        hourly_timestamps = [
            end_date - datetime.timedelta(hours=i) for i in range(7 * 24)
        ]
        if timestamp_dt:
            time_differences = [
                (hourly_ts - timestamp_dt).total_seconds() / 3600
                for hourly_ts in hourly_timestamps
            ]
        else:
            time_differences = [None] * (7 * 24)

        for hourly_ts, time_diff in zip(hourly_timestamps, time_differences):
            data["latitude"].extend(latitudes)
            data["longitude"].extend(longitudes)
            data["current_timestamp"].extend([hourly_ts] * len(latitudes))
            for band in bands:
                if f"{band}" not in data:
                    data[f"{band}"] = []
                if f"{band}_time_diff" not in data:
                    data[f"{band}_time_diff"] = []
                data[f"{band}"].extend(pixel_values)
                data[f"{band}_time_diff"].extend([time_diff] * len(latitudes))

    # Print lengths for debugging
    for key, value in data.items():
        print(f"Length of {key}: {len(value)}")

    # Pad lists to ensure they are all of the same length
    data = pad_lists(data)

    # Convert the data dictionary to a DataFrame and add it to the list
    df = pd.DataFrame(data)
    all_dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
final_df = pd.concat(all_dfs, ignore_index=True)

# Save the final DataFrame to a CSV file
final_df.to_csv("satellite_data_7_day_all.csv", index=False)

print("Data saved to satellite_data_7_day_all.csv")

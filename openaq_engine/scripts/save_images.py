import datetime
import json
import logging
import os

import ee
import pandas as pd
import requests

# Initialize the Earth Engine module.
path_to_private_key = ""
service_account = ""
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
endDate = ee.Date(datetime.datetime.utcnow())

# Define the satellite configuration
SATELLITE_CONFIG = {
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
        "frequency": 30,
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
for satellite in SATELLITE_CONFIG:
    satellite_dict = fetch_and_download_images(
        satellite, SATELLITE_CONFIG[satellite]
    )
    if satellite_dict:
        all_satellite_data.update(satellite_dict)

print("Download tasks have been completed.")
print(all_satellite_data)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    filename="satellite_processing.log",
    filemode="w",
    format="%(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

# Read the uploaded CSV file
file_path = "data/centroid_data.csv"
data = pd.read_csv(file_path)
data = data[:2]


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
    satellite_data, coordinates, hourly_timestamp
):
    closest_time_diff = float("inf")
    closest_pixel_values = {}

    for satellite_key, date_images in satellite_data.items():
        for date, image in date_images.items():
            image_time = datetime.datetime.strptime(date, "%Y-%m-%d_%H-%M-%S")
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
                                scale=SATELLITE_CONFIG[satellite_key][
                                    "resolution"
                                ],
                            )
                            .get(band)
                            .getInfo()
                        )
                        print()

                        if value is not None:  # Check if value is not None
                            closest_pixel_values[band] = value
                            closest_pixel_values[f"{band}_time_diff"] = (
                                time_diff_hours
                            )

    return closest_pixel_values


# Use joblib to parallelize the fetching process with throttling for each location
all_dfs = []
end_date = datetime.datetime.utcnow()
for coordinates in locations:
    results = []
    hourly_timestamps = [
        end_date - datetime.timedelta(hours=i) for i in range(7 * 24)
    ]
    for hourly_ts in hourly_timestamps:
        pixel_values = fetch_pixel_values_from_dict(
            all_satellite_data, coordinates, hourly_ts
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

import time

import ee

credentials = ee.ServiceAccountCredentials(
    "earth-engine@unicef-367711.iam.gserviceaccount.com",
    "/home/ec2-user/openaq-engine/unicef-367711-a4ac0921e063.json",
)  # Initialize the Earth Engine library.
ee.Initialize(credentials)

# Define the geometry for the bounding box.
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

# Filter the MODIS image collection to get the most recent image over Ulaanbaatar.
modis_collection = (
    ee.ImageCollection("MODIS/061/MCD19A2_GRANULES")
    .filterBounds(ulaanbaatar_geometry)
    .sort("system:time_start", False)
    .first()
)

# Define the image as the most recent one in the filtered collection.
image = ee.Image(modis_collection)

# Combine reducers to calculate the mean value and count of pixels within the defined geometry.
reducer = ee.Reducer.mean().combine(
    reducer2=ee.Reducer.count(), sharedInputs=True
)

# Apply the combined reducer to the image over the specified region.
reduced_dict = image.reduceRegion(
    reducer=reducer,
    geometry=ulaanbaatar_geometry,
    scale=1000,  # Appropriate scale for MODIS/061/MCD19A2_GRANULES
)

# Extract the count of pixels from the reduced dictionary.
pixel_count = reduced_dict.getInfo()["Optical_Depth_047_count"]

# Print the number of pixels
print(f"Number of pixels in the specified region: {pixel_count}")

# Convert the result to a feature collection.
feature = ee.Feature(None, reduced_dict).set("pixel_count", pixel_count)
featureCollection = ee.FeatureCollection([feature])

# Export the result as a CSV file to the shared Google Drive folder.
task = ee.batch.Export.table.toDrive(
    collection=featureCollection,
    description="Bounding_Box_MODIS_Pixels",
    fileNamePrefix="Bounding_Box_MODIS_Pixels",
    folder="YOUR_SHARED_FOLDER_ID",  # Replace with your shared folder ID
    fileFormat="CSV",
)
task.start()

# Check the status of the task periodically.
while True:
    task_status = task.status()
    print(task_status)
    if task_status["state"] in ["COMPLETED", "FAILED"]:
        break
    time.sleep(30)  # Check every 30 seconds

if task_status["state"] == "COMPLETED":
    print(
        "Task completed successfully. Check your Google Drive for the CSV file."
    )
else:
    print("Task failed. Please check the details and try again.")

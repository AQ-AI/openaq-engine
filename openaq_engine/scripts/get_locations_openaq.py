import datetime
import json

import ee
import pandas as pd
import requests

url = "https://api.openaq.org/v2/locations?limit=1000&page=1&offset=0&sort=desc&has_geo=true&radius=1000&order_by=lastUpdated&dumpRaw=false"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)
geojson_dict = {"type": "FeatureCollection", "features": []}
service_account = "ali.quidwai@aqai.xyz"
credentials = ee.ServiceAccountCredentials(
    service_account,
    '/home/ec2-user/elevated-watch-399519-d191df2b047f.json',
)
ee.Initialize(credentials)

POPULATION_IMAGE_COLLECTION = (
    "CIESIN/GPWv411/GPW_Basic_Demographic_Characteristics"
)

POPULATION_IMAGE_BAND = ["basic_demographic_characteristics"]

POPULATION_PERIOD = 1100
POPULATION_IMAGE_RES = 1000

NIGHTTIME_LIGHT_IMAGE_COLLECTION: str = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG"
NIGHTTIME_LIGHT_IMAGE_BAND = ["avg_rad"]

NIGHTTIME_LIGHT_PERIOD = 30
NIGHTTIME_LIGHT_RES = 463.83

ee_df_list = []
for _, row in enumerate(json.loads(response.text)["results"]):
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [
                row["coordinates"]["longitude"],
                row["coordinates"]["latitude"],
            ],
        },
        "properties": {"city": row["city"]},
    }
    geojson_dict["features"].append(feature)

    centroid_point = ee.Geometry.Point(
        row["coordinates"]["longitude"], row["coordinates"]["latitude"]
    )

    day_of_interest = ee.Date(datetime.datetime.now())

    image_collection = ee.ImageCollection(NIGHTTIME_LIGHT_IMAGE_COLLECTION).select(
        NIGHTTIME_LIGHT_IMAGE_BAND
    )
    print(image_collection)

    # filtered_image_collection = image_collection.filterDate(day_of_interest.advance(-POPULATION_PERIOD, "days"), day_of_interest)
    # print(filtered_image_collection)
    info = (
        image_collection.filterDate("2010", "2022")
        .getRegion(centroid_point, POPULATION_IMAGE_RES)
        .getInfo()
    )

    ee_df = ee_array_to_df(info, POPULATION_IMAGE_BAND)
    print(ee_df)
    ee_df_list.append(ee_df)

ee_df_full = pd.concat(ee_df_list).reset_index(drop=True)
print(
    "Total population within sensor coverage: ",
    sum(ee_df_full["basic_demographic_characteristics"]),
)

ee_df_full.to_csv("all_population_around_sensors.csv")
with open("result.geojson", "w") as fp:
    json.dump(geojson_dict, fp)

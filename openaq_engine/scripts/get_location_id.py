import datetime
import json
import pandas as pd
import requests
from config.model_settings import TimeSplitterConfig
from src.utils.utils import write_to_db, get_data
from setup_environment import get_dbengine
import time

config = TimeSplitterConfig()
texas_bbox = config.STATE_BOUNDING_BOXES["TX"]
start_date = datetime.datetime.strptime("2018-10-01", "%Y-%m-%d").date()
end_date = datetime.datetime.now().date()

engine = get_dbengine()
location_id_list = []
from_list = True


def get_pm25_data(location_id_list, start_date, end_date):
    for location_id in location_id_list:
        url = f"https://api.openaq.org/v2/measurements?date_from={start_date}&date_to={end_date}&limit=1000&page=4&offset=0&sort=desc&parameter=pm25&radius=100&location_id={location_id}&order_by=datetime"
        headers = {"accept": "application/json"}
        print(url)

        response = requests.get(url, headers=headers)
        data = response.json()
        try:
            results = data["results"]

            df = pd.DataFrame.from_records(results)
            df["latitude"] = df["coordinates"].apply(lambda x: x["latitude"])
            df["longitude"] = df["coordinates"].apply(lambda x: x["longitude"])
            df["date_local"] = df["date"].apply(lambda x: x["local"])
            df["date_utc"] = df["date"].apply(lambda x: x["utc"])
            df = df.drop(["coordinates", "date"], axis=1)
            # df = api_response_to_df(response)
            print(df.head())
            write_to_db(
                df,
                engine,
                "openaq_texas_{start_date}_{end_date}".format(
                    start_date=start_date, end_date=end_date
                ),
                "public",
                "append",
            )
        except KeyError:
            print("No data for location_id: {}".format(location_id))
            pass


def generate_location_id_list():
    for i in range(1, 26):
        url = f"https://api.openaq.org/v2/locations?limit=1000&page={i}&offset=0&sort=desc&parameter=pm25&radius=1000&country=US&order_by=lastUpdated&dumpRaw=false"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        data = response.json()
        print(url)

        results = data["results"]
        for location in results:
            lat = float(location["coordinates"]["latitude"])
            lon = float(location["coordinates"]["longitude"])
            if float(texas_bbox[1][0]) < lon < float(
                texas_bbox[1][2]
            ) and float(texas_bbox[1][1]) < lat < float(texas_bbox[1][3]):
                location_id_list.append(location["id"])


if from_list:
    # locationid_query = f"""select distinct "locationId" from "openaq_texas_{start_date}_{end_date}";""".format(
    #     start_date=start_date, end_date=end_date
    # )
    # locationid_df = get_data(locationid_query)
    # print(locationid_df.head())
    # location_id_completed_list = list(locationid_df.locationId.unique())
    with open("openaq_engine/scripts/texas_loc_id.txt") as f:
        location_id_full_list = f.read().splitlines()
    # print(
    #     "location_id_completed_list",
    #     location_id_completed_list,
    #     "location_id_full_list",
    #     location_id_full_list,
    # )
    # location_id_list = list(
    #     set(location_id_full_list) - set(location_id_completed_list)
    # )
    # print(location_id_list, len(location_id_list))
    # time.sleep(10)
    # with open("openaq_engine/scripts/texas_not_processed_ids1.txt") as f:
    #     location_id_list = f.read().splitlines()
    # print(location_id_list, len(location_id_list))
    get_pm25_data(location_id_full_list, start_date, end_date)
else:
    location_id_list = generate_location_id_list()
    get_pm25_data(location_id_list, start_date, end_date)

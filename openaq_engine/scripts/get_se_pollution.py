import json
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from joblib import Parallel, delayed
from setup_environment import get_dbengine
from shapely.geometry import shape
from src.utils.utils import write_to_db


def check_and_insert(df, engine, table_name, unique_cols):
    """
    Insert records from DataFrame into SQL table if they do not already exist using df.to_sql.

    Parameters:
        df (pd.DataFrame): DataFrame containing the data to insert.
        engine (SQLAlchemy Engine): SQLAlchemy engine object connected to the database.
        table_name (str): Name of the table where data will be inserted.
        unique_cols (list): List of column names in df that uniquely identify each record.
    """
    # Convert list of unique columns into a SQL column string
    columns_sql = ", ".join(unique_cols)

    # Query to select unique columns from the table
    query = f"SELECT DISTINCT {columns_sql} FROM public.{table_name}"

    # Read the existing unique values into a DataFrame
    existing_df = pd.read_sql(query, engine)
    # Create a DataFrame of unique values from the input DataFrame
    unique_df = df[unique_cols].drop_duplicates()
    print("df", df, "unique_df", unique_df)
    # Find rows in unique_df that do not exist in existing_df
    merged_df = pd.merge(
        unique_df, existing_df, on=unique_cols, how="left", indicator=True
    )
    new_rows_df = merged_df[merged_df["_merge"] == "left_only"]

    # Filter the original df to keep only rows that do not exist in the database
    filtered_df = pd.merge(df, new_rows_df[unique_cols], on=unique_cols)

    if not filtered_df.empty:
        # Use df.to_sql to insert the new rows
        filtered_df.to_sql(
            name=table_name, con=engine, if_exists="append", index=False
        )
        print("Data successfully inserted into the database.")
    else:
        print("No new data to insert.")


def extract_coordinates(file_path):
    """
    Extracts latitude and longitude from a GeoJSON file using Shapely.
    Handles both GeoJSON objects and arrays of GeoJSON features.

    Args:
        file_path (str): The path to the GeoJSON file.

    Returns:
        list of tuples: A list of (latitude, longitude) tuples.
    """
    with open(file_path) as file:
        data = json.load(file)
    # Initialize an empty list to hold coordinates
    coordinates = []
    # Check if data is a dictionary with a 'features' key or a direct list of features
    if isinstance(data, dict):
        features = data.get("features", [])
    elif isinstance(data, list):
        features = data
    else:
        raise ValueError(
            "GeoJSON data is neither a feature collection object nor a list of features."
        )
    # Iterate through the features
    for feature in features:
        geom = shape(feature["geometry"])
        # Check geometry type and extract coordinates
        if geom.geom_type == "Point":
            coordinates.append((geom.y, geom.x))
        elif geom.geom_type in ["LineString", "Polygon"]:
            # Extract first coordinate for simplicity
            coordinates.append((geom.coords[0][1], geom.coords[0][0]))
        elif geom.geom_type == "MultiPolygon":
            # Extract first coordinate of the first polygon
            coordinates.append(
                (geom[0].exterior.coords[0][1], geom[0].exterior.coords[0][0])
            )
    return coordinates


def api_with_response_df(url):
    """
    Fetch data from the provided URL and return a DataFrame and the full response.
    Implements retry with exponential backoff on rate limiting.
    """
    headers = {"accept": "application/json"}
    max_retries = 5
    retry_count = 0
    base_wait_time = 10  # Base wait time in seconds

    while retry_count < max_retries:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return (
                pd.json_normalize(response.json()["results"]),
                pd.DataFrame(json.loads(response.text)["results"]),
                response.text,
            )
        elif response.status_code == 429:
            wait_time = base_wait_time * (2**retry_count)
            print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retry_count += 1
        else:
            print(
                f"API Error: {response.json().get('detail', 'No details provided.')}"
            )
            return None, None, None
    print("Max retries exceeded. Unable to fetch data.")
    return None, None, None


def query_sensor_measurements(lat, lon, date_from, date_to):
    current_date = (
        datetime.strptime(date_from, "%Y-%m-%d")
        if isinstance(date_from, str)
        else date_from
    )

    # Set end_date to the minimum of today's date or date_to
    potential_end_date = (
        datetime.strptime(date_to, "%Y-%m-%d")
        if isinstance(date_to, str)
        else date_to
    )
    end_date = min(potential_end_date, datetime.now())

    all_data = []

    while current_date <= end_date:
        # Calculate the end of the current segment, which is either 30 days from current_date or the end_date, whichever comes first
        next_month_date = current_date + timedelta(days=30)
        segment_end_date = min(next_month_date, end_date)

        # Construct URL with the current segment's date range
        url = f"https://api.openaq.org/v2/measurements?date_to={segment_end_date.strftime('%Y-%m-%d')}&date_from={current_date.strftime('%Y-%m-%d')}&limit=10000&sort=desc&coordinates={lat},{lon}&radius=25000&order_by=datetime"
        print("Fetching data for URL:", url)

        json_ser, df, response = api_with_response_df(url)

        # Adjust any nested DataFrame columns if necessary
        df = adjust_nested_column(df)

        # Connect to database and insert new data
        engine = get_dbengine()
        unique_cols = [
            "location",
            "date",
        ]  # Adjust based on DataFrame and table schema
        check_and_insert(
            df, engine, "cohorts_southeast_measurements", unique_cols
        )

        # Logging the result of the fetch
        if df is not None and not df.empty:
            all_data.append(df)
            print(
                f"Data retrieved and processed for period {current_date.strftime('%Y-%m-%d')} to {segment_end_date.strftime('%Y-%m-%d')}."
            )
        else:
            print(
                f"No data or API error for period {current_date.strftime('%Y-%m-%d')} to {segment_end_date.strftime('%Y-%m-%d')}."
            )

        # Move the current_date forward to the next segment
        current_date = segment_end_date + timedelta(days=1)

    return (
        pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    )


def query_openaq_pollution(coordinates):
    """
    Retrieves pollution data from the OpenAQ API.
    """
    url = f"https://api.openaq.org/v2/locations?page=1&offset=0&sort=desc&coordinates={coordinates[0]}%2C{coordinates[1]}&radius=25000&order_by=lastUpdated&dump_raw=false"
    print(url)
    json_ser, locations_df, response = api_with_response_df(url)
    try:
        sensor_coords_list = list(
            json.loads(response)["results"][0]["coordinates"].values()
        )
        start_date = datetime.strptime(
            json.loads(response)["results"][0]["firstUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        )
        end_date = datetime.strptime(
            json.loads(response)["results"][0]["lastUpdated"],
            "%Y-%m-%dT%H:%M:%S+00:00",
        )

        measurement_df = query_sensor_measurements(
            sensor_coords_list[0], sensor_coords_list[1], start_date, end_date
        )
        return locations_df, measurement_df
    except Exception:
        logging.exception("Error in fetching data")
        return None, None


def _results_to_db(df, engine, location):
    """Write model results to the database for all cohorts"""
    write_to_db(
        df,
        engine,
        f"cohorts_{location}",
        "public",
        "append",
    )


def adjust_nested_column(df):
    """
    Remove duplicate coordinates from a list of coordinates.
    """
    df["date"] = df["date"].apply(
        lambda x: x["utc"] if isinstance(x, dict) and "utc" in x else None
    )

    # Extract 'latitude' and 'longitude' into separate new columns
    df["latitude"] = df["coordinates"].apply(
        lambda x: x["latitude"] if isinstance(x, dict) else None
    )
    df["longitude"] = df["coordinates"].apply(
        lambda x: x["longitude"] if isinstance(x, dict) else None
    )

    # Now drop the original 'coordinates' column
    df.drop("coordinates", axis=1, inplace=True)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Usage example
    file_path = "/home/ec2-user/openaq-engine/data/mills.geojson"
    coordinates = extract_coordinates(file_path)
    results = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
        delayed(query_openaq_pollution)(coords) for coords in coordinates
    )

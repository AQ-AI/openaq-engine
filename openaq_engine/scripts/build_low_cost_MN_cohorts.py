import json
import os

import pandas as pd
from setup_environment import get_dbengine

# Set up the database connection
engine = get_dbengine(
    os.getenv("PGDATABASE"),
    os.getenv("PGHOST"),
    os.getenv("PGPORT"),
    os.getenv("PGUSER"),
    os.getenv("PGPASSWORD"),
)


# Function to read a table from the database
def read_table(table_name, schema):
    query = f'SELECT * FROM {schema}."{table_name}"'
    return pd.read_sql_query(query, engine)


# Function to convert datetime to specified format
def convert_datetime(datetime_series):
    return datetime_series.apply(
        lambda x: {"utc": x.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "local": ""}
    )


# Function to translate x, y to latitude and longitude
def translate_coordinates(x, y):
    return {"latitude": y, "longitude": x}


def clean_json_string(json_str):
    """
    Cleans a JSON string by removing extra quotes and backslashes.

    Parameters:
    json_str (str): The JSON string to clean.

    Returns:
    dict: The cleaned JSON as a dictionary.
    """
    if isinstance(json_str, str):
        try:
            cleaned_str = json_str.replace('\\"', '"')
            cleaned_str = cleaned_str.strip('"')
            return json.loads(cleaned_str)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON string: {json_str}")
    return json_str


# Example usage with a DataFrame
def clean_dataframe(df, columns_to_clean):
    """
    Cleans specified columns in a DataFrame by removing extra quotes and backslashes from JSON strings.

    Parameters:
    df (pd.DataFrame): The DataFrame to clean.
    columns_to_clean (list of str): List of column names to clean.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    for column in columns_to_clean:
        df[column] = df[column].apply(clean_json_string)
    return df


# Function to generate locationId
def generate_location_ids(df, existing_locations):
    location_id_map = {}
    next_location_id = (
        max(existing_locations.values()) + 1 if existing_locations else 1
    )

    def get_location_id(lat, lon):
        for (existing_lat, existing_lon), loc_id in existing_locations.items():
            if lat == existing_lat and lon == existing_lon:
                return loc_id
        nonlocal next_location_id
        location_id_map[(lat, lon)] = next_location_id
        existing_locations[(lat, lon)] = next_location_id
        next_location_id += 1
        return location_id_map[(lat, lon)]

    df["locationId"] = df.apply(
        lambda row: get_location_id(row["y"], row["x"]), axis=1
    )
    return df, existing_locations


# Function to process and transform a dataframe
def process_dataframe(df, existing_locations, target_columns):
    # Convert datetime column to specified format
    df["datetime"] = pd.to_datetime(df["Datetime"])
    df["date"] = convert_datetime(df["datetime"])

    # Generate locationId
    df, existing_locations = generate_location_ids(df, existing_locations)

    # Translate x, y to latitude and longitude
    df["coordinates"] = df.apply(
        lambda row: translate_coordinates(row["x"], row["y"]), axis=1
    )

    # Add isMobile, isAnalysis, sensorType columns
    df["isMobile"] = "f"
    df["isAnalysis"] = None
    df["sensorType"] = None
    df["parameter"] = "pm25"
    df["city"] = "Ulaanbaatar"

    df = df[target_columns]

    return df, existing_locations


if __name__ == "__main__":

    # Read the local_MN_data table
    local_mn_data_df = read_table("local_MN_data", "public")
    columns_to_clean = ["date", "coordinates"]
    local_mn_data_df = clean_dataframe(local_mn_data_df, columns_to_clean)

    target_columns = local_mn_data_df.columns

    # Extract existing locations and their IDs from local_MN_data
    existing_locations = {}
    for _, row in local_mn_data_df.iterrows():
        coordinates = row["coordinates"]
        if isinstance(coordinates, str):
            coordinates = json.loads(coordinates)
        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")
        existing_locations[(lat, lon)] = row["locationId"]

    # Directory containing the CSV files
    directory = "data/PurpleAir_Cleaned_1"

    # Initialize an empty list to hold all processed dataframes
    all_dfs = [local_mn_data_df]

    # Iterate through all CSV files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[
                0
            ]  # Remove the .csv extension
            print(f"Processing table: {table_name}")

            # Read the table from the database
            df = read_table(table_name, "raw")
            df = df[df["indoor_outdoor"].str.contains("outside", na=False)]
            # Process and transform the dataframe
            df, existing_locations = process_dataframe(
                df, existing_locations, target_columns
            )

            # Append the transformed dataframe to the list
            all_dfs.append(df)

    # Concatenate all dataframes
    concatenated_df = pd.concat(all_dfs, ignore_index=True)
    # Convert 'coordinates' and 'datetime' columns to JSON strings
    concatenated_df["coordinates"] = concatenated_df["coordinates"].apply(
        json.dumps
    )
    concatenated_df["date"] = concatenated_df["date"].apply(json.dumps)

    # Save the concatenated dataframe to a new table in the database
    concatenated_df.to_sql(
        "low_cost_MN_data",
        engine,
        schema="public",
        if_exists="replace",
        index=False,
    )

    print(
        "Data concatenation and transformation complete. The data has been saved to the 'concatenated_data' table in the database."
    )

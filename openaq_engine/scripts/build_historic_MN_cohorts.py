import json
import re

import numpy as np
import pandas as pd
from setup_environment import get_dbengine
from src.utils.utils import write_to_db


def read_local_data(local_table_name, chunksize=1000):
    # Format the SQL query with the table name enclosed in double quotes to handle special characters
    query = f"""
    SELECT * FROM "{local_table_name}"
    WHERE
        date IS NOT NULL AND date != ''
        AND value IS NOT NULL
        AND coordinates IS NOT NULL AND coordinates != ''
    """.strip()

    return pd.read_sql_query(query, con=get_dbengine(), chunksize=chunksize)


def correct_and_convert_json_coordinates(coord_str):
    # Remove extraneous braces and whitespace
    corrected_str = coord_str.strip("{} ")

    # Correctly add quotes around keys and the colon separator
    corrected_str = re.sub(r"(\b\w+\b)(=)", r'"\1":', corrected_str)

    # Ensure that the values are formatted correctly as decimals (and are not quoted if numeric)
    corrected_str = re.sub(r"(:\s*)(\d+\.\d+)", r"\1\2", corrected_str)

    # Rebuild the JSON string properly
    final_json_str = "{" + corrected_str + "}"

    try:
        # Convert the corrected string to a JSON object
        data = json.loads(final_json_str)
        return json.dumps(
            data
        )  # Return the string representation of the JSON object
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {final_json_str}")
        print(f"Error: {e}")
        return None


def convert_dates_json(date_str):
    # Strip extraneous braces and whitespace
    corrected_str = date_str.strip("{} ")

    # Quote the keys explicitly
    corrected_str = re.sub(r"(\b\w+\b)(=)", r'"\1":', corrected_str)

    # Properly format datetime strings by avoiding breaks inside them
    corrected_str = re.sub(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)",
        r'"\1"',
        corrected_str,
    )

    # Correctly quote all other values ensuring no extra quotes disrupt the datetime formatting
    corrected_str = re.sub(
        r'(?<!:)"\s*([^",]+?)\s*"', r"\1", corrected_str
    )  # Remove erroneous quotes
    corrected_str = re.sub(
        r':\s*([^",]+?)\s*(,|$)', r': "\1"\2', corrected_str
    )  # Ensure remaining values are quoted
    corrected_str = re.sub(
        r"(\b(utc|local)\b)(?=\s*:)", r'"\1"', corrected_str
    )
    # Handle the final JSON structure
    corrected_str = "{" + corrected_str + "}"

    try:
        data = json.loads(corrected_str)
        return json.dumps(data)
    except json.JSONDecodeError as e:
        print(f"Failed to decode: {corrected_str}")
        print(f"Error: {e}")
        return None


def location_id_map(df):
    # Predefined IDs for specific locations
    location_to_id = {"Amgalan": 23, "US Diplomatic Post: Ulaanbaatar": 8160}

    # Step 1: Extract unique locations
    unique_locations = df["location"].unique()

    # Step 2: Create a mapping from location names to IDs, skipping already defined locations
    next_id = (
        max(location_to_id.values(), default=0) + 1
    )  # Start next ID after the highest predefined ID
    for location in unique_locations:
        if location not in location_to_id:
            location_to_id[location] = next_id
            next_id += 1

    # Step 3: Map the location names to IDs in the DataFrame
    df["locationId"] = df["location"].map(location_to_id)
    return df


def replace_units(df):
    """
    Replaces 'Âµg/mÂ³' with 'µg/m³' in the 'unit' column of the DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the 'unit' column.

    Returns:
    pandas.DataFrame: The updated DataFrame with the replaced values.
    """
    df["unit"] = df["unit"].str.replace("Âµg/mÂ³", "µg/m³", regex=False)
    return df


if __name__ == "__main__":
    for df_chunk in read_local_data(
        "Ulaanbaatar Particulate Matter Sensor Data"
    ):
        # Apply the function to the date column
        df_chunk["date"] = df_chunk["date"].apply(convert_dates_json)

        # Applying the correction within the DataFrame
        df_chunk["coordinates"] = df_chunk["coordinates"].apply(
            correct_and_convert_json_coordinates
        )

        # Convert 'mobile' from 'f'/'t' to boolean
        df_chunk["isMobile"] = (
            df_chunk["mobile"].map({"false": False, "true": True}).astype(bool)
        )
        df_chunk = replace_units(df_chunk)

        # Convert location names to location IDs
        df_chunk = location_id_map(df_chunk)
        df_chunk["city"] = df_chunk["city"]
        df_chunk["location"] = df_chunk["location"]

        # Reorganize columns according to the new schema
        new_df = df_chunk.rename(
            columns={"value": "value", "sourcetype": "entity"}
        ).assign(
            isAnalysis=np.nan,  # assigning NaN to the isAnalysis column
            sensorType=np.nan,
        )[
            [
                "locationId",
                "location",
                "city",
                "parameter",
                "value",
                "date",
                "unit",
                "coordinates",
                "country",
                "city",
                "isMobile",
                "isAnalysis",
                "entity",
                "sensorType",
            ]
        ]

        # Write to database
        write_to_db(
            new_df, get_dbengine(), "local_MN_data", "public", "append"
        )

# Get all location IDs for purple air sensors (in Laos)
import requests
import json
import subprocess

def download_data(parsed_locations):
    for location in parsed_locations:
        location_id = location['location_id']
        datetime_first_year = location['datetime_first_year']
        # Construct the AWS CLI command
        source_path = f"s3://openaq-data-archive/records/csv.gz/locationid={location_id}/year={datetime_first_year}"
        destination_path = f"s3://laos-purpleair/{location_id}-{datetime_first_year}"        
        command = f"aws s3 cp {source_path} {destination_path} --recursive"

        try:
            # Execute the command
            subprocess.run(command, shell=True, check=True)
            print(f"Data for location ID {location_id} in year {datetime_first_year} has been downloaded.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to download data for location ID {location_id} in year {datetime_first_year}: {e}")


def parse_purpleair_locations(response_text):
    # Convert the JSON-formatted string to a dictionary
    json_data = json.loads(response_text)
    
    # Initialize a list to hold the parsed data
    parsed_data = []
    
    # Check for the 'results' key in the parsed JSON
    if 'results' in json_data:
        for location in json_data['results']:
            # Check if the provider name is "PurpleAir"
            if location['provider']['name'] == "PurpleAir":
               location_id = location['id']
                
               # Extract the years from datetimeFirst and datetimeLast
               # Assuming these fields exist in the location or instrument details
               # and are in ISO 8601 format (e.g., "2021-03-04T00:00:00Z")
               datetime_first_year = location.get('datetimeFirst', {}).get('utc', '')[:4]
               datetime_last_year = location.get('datetimeLast', {}).get('utc', '')[:4]

               # Append the extracted data to the list
               parsed_data.append({
                    'location_id': location_id,
                    'datetime_first_year': datetime_first_year,
                    'datetime_last_year': datetime_last_year,
                })

    return parsed_data
laos_id = 98

url = f"https://api.openaq.org/v3/locations?order_by=id&sort_order=asc&countries_id=98&limit=200&page=1"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)
print(response.text)
parsed_data = parse_purpleair_locations(response.text)

download_data(parsed_data)

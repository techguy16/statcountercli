import os
import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta
import argparse

# Argument parser setup
parser = argparse.ArgumentParser(description='StatCounterCli - Fetch and process StatCounter data into JSON data.')
parser.add_argument('-d', '--specific-id', type=str, help='Fetch data for a specific ID from the JSON file')
parser.add_argument('-a', '--all', action='store_false', help='Fetch data for all available dataspans (default)')
args = parser.parse_args()

# Get current date
current_date = datetime.now()
current_year_month = current_date.strftime('%Y%m')
current_year_month_dash = current_date.strftime('%Y-%m')

# Check if it's the first day of the month
if current_date.day == 1:
    # Set the date to the first day of the previous month
    previous_month = current_date.replace(day=1) - timedelta(days=1)
    current_year = previous_month.year
    current_month = previous_month.month
    to_date = previous_month
else:
    current_year = current_date.year
    current_month = current_date.month
    to_date = current_date

# Read the JSON file containing URL data
with open('generated_urls.json', 'r') as json_file:
    urls_data = json.load(json_file)

# Access the list of URLs under the key 'URLs'
for item in urls_data['URLs']:
    if args.all or (args.specific_id and args.specific_id == item['ID']):
        url = item['URL']
        name = item['Name']
        output_file_name = item['Output']

        # Replace placeholders in URL
        url = url.replace("$currentDate", current_year_month)
        url = url.replace("$DateWithDash", current_year_month_dash)
        print("Getting Dataset ", name)
        print("URL of data: ", url)

        # Fetch data from the URL
        response = requests.get(url)

        # Use io.StringIO to read CSV data
        data = pd.read_csv(io.StringIO(response.text))

        # Convert data to dictionary with specified format
        output_data = {}
        for index, row in data.iterrows():
            date = row['Date']
            if date not in output_data:
                output_data[date] = {}
            for column in data.columns[1:]:
                value = row[column]
                if value != 0:
                    output_data[date][column] = value

                    # Sort subitems within each date key by their values
                    output_data[date] = dict(sorted(output_data[date].items(), key=lambda item: item[1], reverse=True))

        # Convert to valid JSON
        json_data = json.dumps(output_data, indent=2)

        # Check if output directory exists, if not, create it
        output_dir = os.path.dirname(output_file_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Create and write data to output JSON file
        with open(output_file_name, 'w') as output_file:
            output_file.write(json_data)

        print(f"Data fetched and saved to '{output_file_name}'")


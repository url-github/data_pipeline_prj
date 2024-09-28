'''
Pobieram z API do formatu csv i ładuje do GCS
'''

import os
import requests
import csv
from google.cloud import storage
from google.oauth2 import service_account

service_account_key_path = '/Users/p/Documents/SA/605074532510-compute.json'

credentials = service_account.Credentials.from_service_account_file(service_account_key_path)

url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
    "X-RapidAPI-Key": "https://rapidapi.com/hub",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    'formatType': 't20'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']

        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)

            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")

        bucket_name = '012-ranking-data'
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)
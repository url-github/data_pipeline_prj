'''
Pobieram z API do formatu csv
'''

import requests
import csv

url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
    "X-RapidAPI-Key": "https://rapidapi.com/hub",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    # 'formatType': 'odi'
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
            # writer.writeheader()

            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)
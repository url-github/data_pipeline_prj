import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "xxxxxxxxxxxxxxxxx"
TOKEN = "xxxxxxxxxxxxxxxxx"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Sprawdź, czy dataframe jest pusty
    if df.empty:
        print("No songs downloaded. Finishing execution") # Nie pobrano żadnych utworów.
        return False

    # Sprawdzenie klucza podstawowego (Primary Key Check)
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated") # Kontrola klucza podstawowego jest naruszona

    # Sprawdź, czy nie ma wartości null
    if df.isnull().values.any():
        raise Exception("Null values found") # Znaleziono null

    # Sprawdź, czy wszystkie znaczniki czasu pochodzą z wczorajszej daty
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
            # Co najmniej jeden ze zwróconych utworów nie ma wczorajszego znacznika czasu

    return True

if __name__ == "__main__":

    # Wyodrębnij część procesu ETL

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f'Bearer {TOKEN}'
    }

    # Konwertuj czas na uniksowy znacznik czasu w milisekundach
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Pobierz wszystkie utwory, których słuchałeś „po wczoraj”, czyli w ciągu ostatnich 24 godzin
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Wyodrębnianie tylko odpowiednich danych z obiektu json
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Przygotuj słownik, aby przekształcić go w data frame poniżej
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)
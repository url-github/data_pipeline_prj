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
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary Key check is violated")

    if df.isnull().values.any():
        raise Exception("Null values found")

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        timestamp_date = datetime.datetime.strptime(timestamp, '%Y-%m-%d').date()
        if timestamp_date != yesterday:
            print(f"Warning: A song with timestamp {timestamp} is not from yesterday.")

    return True

if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f'Bearer {TOKEN}'
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    if check_if_valid_data(song_df):
        print("Dane prawidłowe, przejdź do etapu ładowania (Load)")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Pomyślnie otwarto bazę danych")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Dane już istnieją w bazie danych")

    conn.close()
    print("Zamknij pomyślnie bazę danych")

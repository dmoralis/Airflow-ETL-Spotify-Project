import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True


def spotify_etl():
    # Extract part of the ETL process
    database_location = "postgresql://airflow:airflow@localhost:8080/airflow"
    user_id = "bcfddd386ab24c9195e1b515585af30a"  # your Spotify username
    token = "BQCWiVV3LKJsfEuIQXNkg136EndK3pR4-ish_Rb9nMDlp85p386uec3Y65zCimsHY3sxnF0KfEbZSzlCKcxIzhnoNSjs_e7DlsLEq_9K_KjSDJS1QPg"  # your Spotify API token

    headers = {
        #"Accept": "application/json",
        #"Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    # Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    start_date = today - datetime.timedelta(days=30)
    yesterday_unix_timestamp = int(start_date.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    #r = requests.get(
    #    "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp),
    #    headers=headers)
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played",
        headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    try:
        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])
    except KeyError as e:
        print(headers)
        print(f'Items not found because data is {data}')
    # Prepare a dictionary in order to turn it into a pandas dataframe below
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(database_location)
    conn = engine.connect()
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS played_tracks(
            id SERIAL PRIMARY KEY,
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
        )
        """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()

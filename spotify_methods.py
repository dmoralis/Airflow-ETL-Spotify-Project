import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import base64
from urllib.parse import urlencode, urlparse, parse_qs


CLIENT_ID = "**"
CLIENT_SECRET = "**"
REDIRECT_URI = "http://localhost:8888/callback"
DATABASE_LOCATION = "postgresql://airflow:airflow@a39e18f45b63:5432/airflowspotify-postgres-1"
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

def request_token():
    # Check if a file containing the refresh token exists
    if open('refresh_token.txt', "r"):
        f = open('refresh_token.txt', "r")
        refresh_token = f.readline()
    else:
        print('No file refresh_token.txt found')
        return -1

    # Request the token using the refresh token instead of authentication code
    token_url = 'https://accounts.spotify.com/api/token'
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    headers = {
        'Authorization': f'Basic {CLIENT_ID}:{CLIENT_SECRET}'
    }
    response = requests.get(token_url, data=data, headers=headers).json()
    if response.status_code == 200:
        token = response.get('access_token')
    else:
        print(f'Token could not be taken due to error code: {response.status_code}')
        return -1

    # Using the token now request the wanted data
    response = requests.get('https://api.spotify.com/v1/me/player/recently-played', header={'Authorization':
                                                                                                f'Bearer {token}'})
    return response

def request_spotify_refresh_token():
    print(f'This methods require from the user to open a link and paste it in the runtime terminal, in order to '
          f'proceed to the request of the refresh token')

    auth_url = 'https://accounts.spotify.com/authorize?'
    scope = 'user-read-private'
    data = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'scope': scope,
        'redirect_uri': REDIRECT_URI}
    auth_url = auth_url + '?' + urlencode(query=data)

    print(f'You need to open this {auth_url} url in your browser, while you are logged on your spotify account, and get the url '
          f'result')
    auth_answer = input("Insert the link that resulted in the URL:")
    auth_code = parse_qs((urlparse(auth_answer)).query)['data'][0]

    token_url = 'https://accounts.spotify.com/api/token'
    data = {
        'code': auth_code,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    headers = {
        'Authorization': 'Basic' + f'{CLIENT_ID}:{CLIENT_SECRET}'
    }

    response = requests.get(token_url, data=data, headers=headers).json()

    if response.status_code == 200:
        print('Successful request of the token..\n Proceeds in saving the refresh token in a refresh_token.txt file')
        with open('refresh_token.txt', "w+") as refresh_token_file:
            refresh_token_file.write(response.get('refresh_token'))
        print('Save is done\nTrying out if the refresh token works')

    else:
        print(f'The token request failed with status code:{response.status_code}')



def spotify_etl():
    # Request data from Spotify API
    data = request_token()
    if data == -1:
        print('Reconfiguring the tokens')
        request_spotify_refresh_token()
        data = request_token()
    data = data.json()

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
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
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

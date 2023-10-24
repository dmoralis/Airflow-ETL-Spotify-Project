import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd
import requests
from collections import defaultdict
import json
from datetime import datetime
import datetime
import sqlite3
import base64
import os
from urllib.parse import urlencode, urlparse, parse_qs


CLIENT_ID = "e0**"
CLIENT_SECRET = "e5**"
REDIRECT_URI = "http://localhost:8888/callback"
REFRESH_TOKEN = "AQ**"
DATABASE_LOCATION = "postgresql://airflow:airflow@a39e18f45b63:5432/airflow"

#connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
def check_if_valid_data(df: pd.DataFrame, ) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    #if pd.Series(df['played_at']).is_unique:
    #    pass
    #else:
    #    raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        print(df)
        raise Exception("Null values found")

    '''# Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")'''

    return True


def request_token_without_authentication(refresh_token=None, api_link='https://api.spotify.com/v1/me/'
                                                                        'player/recently-played'):
    # Check if a file containing the refresh token exists
    if refresh_token is None:
        refresh_token = REFRESH_TOKEN

    # Request the token using the refresh token instead of authentication code
    token_url = 'https://accounts.spotify.com/api/token'
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    }
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        token = response.json().get('access_token')
    else:
        print(f'Token could not be taken due to error code: {response.status_code}')
        return response

    # Using the token now request the wanted data
    response = requests.get(api_link, headers={'Authorization': f'Bearer {token}'})
    return response


def request_refresh_token_with_authentication():
    print(f'This methods require from the user to open a link and paste it in the runtime terminal, in order to '
          f'proceed to the request of the refresh token')

    auth_url = 'https://accounts.spotify.com/authorize?'
    scope = 'user-read-private user-read-email user-modify-playback-state user-read-playback-position' \
            ' user-library-read streaming user-read-playback-state user-read-recently-played playlist-read-private'
    data = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'scope': scope,
        'redirect_uri': REDIRECT_URI}
    auth_url = f"{auth_url}{urlencode(data)}"

    print(f'You need to open this {auth_url} url in your browser, while you are logged on your spotify account,'
          f' and get the url result')
    auth_answer = input("Insert the link that resulted in the URL:")
    auth_code = parse_qs((urlparse(auth_answer)).query)['code'][0]

    token_url = 'https://accounts.spotify.com/api/token'
    data = {
        'code': auth_code,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    }

    respo = requests.post(token_url, data=data, headers=headers)
    if respo.status_code == 200:
        respo = respo.json()
        refresh_token = respo.get('refresh_token')
        print(f'Successful request of the token..\nPlease insert it in the constant variables and run the Airflow DAG \n{refresh_token}')
        print('Trying out if the refresh token works')
        response_test = request_token_without_authentication(refresh_token=refresh_token)
        if response_test.status_code == 200:
            print('The new refresh token works')
        else:
            print(f'The new refresh token doesn\'t work with error code: {response_test.status_code}. Please check the'
                  f'credentials and try again')
    else:
        print(f"The token request failed with status code:{respo.status_code}")


def fetch_data_and_create_tables(**kwargs):
    ti = kwargs['ti']

    # Request data from Spotify API
    print('ZERO STAGE')
    data = request_token_without_authentication()
    if data.status_code != 200:
        print('No file refresh_token.txt found, please run \'request_spotify_with_authentication\' first locally.')
        return -1

    data = data.json()

    song_dict = defaultdict(list)
    artist_dict = defaultdict(list)
    streaming_dict = defaultdict(list)

    print('FIRST STAGE')
    # Create song dictionary w/ relevant infos
    try:
        for song in data["items"]:
            artist_id = song["track"]["artists"][0]["id"]

            # Set listens info dictionary
            streaming_dict["track_id"].append(song["track"]["id"])
            streaming_dict["artist_id"].append(artist_id)
            streaming_dict["played_timestamp"].append(song["played_at"])

            # Set song info dictionary
            song_dict['id'].append(song["track"]["id"])
            song_dict['song_name'].append(song["track"]["name"])
            song_dict['artist_name'].append(song["track"]["artists"][0]["name"])
            song_dict['duration'].append(song["track"]["duration_ms"])
            song_dict['is_explicit'].append(song["track"]["explicit"])
            song_dict['is_in_album'].append(True if song["track"]["album"] == 'album' else False)

            # Set artist info dictionary
            if artist_id not in artist_dict["id"]:
                artist_response = request_token_without_authentication(api_link=f"https://api.spotify.com/v1/artists/"
                                                                                f"{artist_id}").json()
                artist_dict['id'].append(artist_id)
                artist_dict['followers'].append(artist_response["followers"]["total"])
                artist_dict['genres'].append(artist_response["genres"])
                artist_dict['popularity'].append(artist_response["popularity"])
                artist_dict['name'].append(artist_response["name"])

    except KeyError as e:
        print(f'Items not found because data is {data} \n ERROR ON {e}')

    print(f'Song dict test print {song_dict}')
    song_df = pd.DataFrame(song_dict, columns=["id", "song_name", "artist_name", "duration",
                                               "is_explicit", "is_in_album"])
    artist_df = pd.DataFrame(artist_dict, columns=["id", "followers", "genres", "popularity", "name"])

    streaming_df = pd.DataFrame(streaming_dict, columns=["track_id", "artist_id", "played_timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Songs Data valid, proceed to Load stage")

    if check_if_valid_data(artist_df):
        print("Artists Data valid, proceed to Load stage")

    print('THIRD STAGE')
    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    Session = sessionmaker(bind=engine)
    session = Session()
    print('FOURTH STAGE')
    sql_query_create_tables = """
        DROP TABLE IF EXISTS played_tracks;
        DROP TABLE IF EXISTS streaming;
        DROP TABLE IF EXISTS artists;
        
        CREATE TABLE IF NOT EXISTS played_tracks(
            id VARCHAR(200) PRIMARY KEY,
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            duration INT,
            is_explicit BOOLEAN,
            is_in_album BOOLEAN
        );
        
        CREATE TABLE IF NOT EXISTS streaming(
            track_id VARCHAR(200),
            artist_id  VARCHAR(200),
            played_timestamp timestamp,
            ingestion_date timestamp DEFAULT current_timestamp,
            PRIMARY KEY (track_id, artist_id, played_timestamp)
        );
        
        CREATE TABLE IF NOT EXISTS artists(
                id VARCHAR(200) PRIMARY KEY,
                followers INT,
                genres text[],
                popularity smallint,
                name VARCHAR(200)
        );
        """
    session.execute(sql_query_create_tables)
    print("Opened database successfully")

    # Use a raw SQL query to list databases
    result = session.execute("SELECT datname FROM pg_database;")

    q = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    result = session.execute(q)
    # Retrieve and print the database names
    for row in result:
        print(row[0])

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    # pushes data in any_serializable_value into xcom with key "identifier as string"
    ti.xcom_push(key="song_df", value=song_df)
    ti.xcom_push(key="artist_df", value=artist_df)
    ti.xcom_push(key="streaming_df", value=streaming_df)

    session.commit()
    session.close()

def transform_and_load_data(**kwargs):
    ti = kwargs['ti']

    song_df = ti.xcom_pull(key="song_df")
    artist_df = ti.xcom_pull(key="artist_df")
    streaming_df = ti.xcom_pull(key="streaming_df")
    print(f'Song df {song_df} \n\n artist df {artist_df} \n\n streaming df {streaming_df}')

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in song_df.iterrows():
        session.execute(f"INSERT INTO played_tracks(id, song_name, artist_name, duration, is_explicit, is_in_album) "
                        f"VALUES ('{row['id']}', '{row['song_name']}', '{row['artist_name']}', {row['duration']}, "
                        f"{row['is_explicit']}, '{row['is_in_album']}') "
                        f"ON CONFLICT (id) DO NOTHING")

    for index, row in artist_df.iterrows():
        session.execute(f"INSERT INTO artists(id, name, followers, genres, popularity) "
                        f"VALUES('{row['id']}', '{row['name']}', {row['followers']}, ARRAY{row['genres']},"
                        f" {row['popularity']}) ON CONFLICT (id) DO NOTHING")

    for index, row in streaming_df.iterrows():
        session.execute(f"INSERT INTO streaming(track_id, artist_id, played_timestamp) "
                        f"VALUES('{row['track_id']}', '{row['artist_id']}', '{row['played_timestamp']}') "
                        f"ON CONFLICT (track_id, artist_id, played_timestamp) DO NOTHING")



    print('Data were loaded in the tables')
    for temp in session.execute(f"SELECT * FROM artists;"):
        print(temp)

    print('QUERY2')
    for temp in session.execute(f"SELECT * FROM played_tracks;"):
        print(temp)

    print('QUERY3')
    for temp in session.execute(f"SELECT * FROM streaming;"):
        print(temp)

if __name__ == '__main__':
    print('dummy')
    #request_refresh_token_with_authentication()
    #response = request_token()
    #print('dummy print')
    #spotify_etl()
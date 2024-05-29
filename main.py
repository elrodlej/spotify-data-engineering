# importing the libraries that will get used
import os
import json
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':
# Defining variables that will get used later
 today = datetime.now()
 yesterday = today - timedelta(days=2)
 yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
 yesterday_formatted = (yesterday + timedelta(days=1)).strftime('%Y-%m-%d')

# Load environmental variables
 load_dotenv('D:\VsCodeDatas\Spotify DE\.env', override=True)

# Create variables using the .env file
 CLIENT_ID = os.environ.get('CLIENT_ID')
 CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
 REDIRECT_URI = os.environ.get('REDIRECT_URI')
 SCOPE = os.environ.get('SCOPE')
 PASSWORD = os.environ.get('PASSWORD')
 USER = os.environ.get('USER')
 HOST = os.environ.get('HOST')
 PORT = os.environ.get('PORT')
 SRC_DB = os.environ.get('SRC_DB')

# Create the connection to the DB
 engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{SRC_DB}')

#Create a cache handler for token and specify Spotify Oauth
 cache_handler = MemoryCacheHandler()
 sp_oauth = SpotifyOAuth(
  client_id=CLIENT_ID,
  client_secret=CLIENT_SECRET,
  redirect_uri=REDIRECT_URI,
  scope=SCOPE,
  cache_handler=cache_handler,
  show_dialog=True)

#Create a variable to hold the Spotify object
 sp = Spotify(auth_manager=sp_oauth)

# Retrieve the desired data from the spotify account
 playlists = sp.current_user_recently_played(after=yesterday_unix_timestamp)

 #Convert the data to json for later use
 with open("playlists.json", "w") as outfile:
  json.dump(playlists, outfile)

 #Open the json file and load the data
 json_file = 'D:\VsCodeDatas\Spotify DE\playlists.json'
 with open(json_file, 'r') as file:
  json_playlists = json.load(file)

# Create empty variables for later use
 song_names = []
 artist_names = []
 played_at_list = []
 timestamps = []

#Store the file data inside the empty variables previously defined
 for song in json_playlists['items']:
  song_names.append(song["track"]["name"])
  artist_names.append(song["track"]["album"]["artists"][0]["name"])
  played_at_list.append(song["played_at"])
  timestamps.append(song["played_at"][0:10])

# Create a dict with the previously defined lists
 song_dict = {
  "song_name" : song_names,
  "artist_name" : artist_names,
  "played_at" : played_at_list,
  "timestamp" : timestamps
 }

# Extract the columns that will be used for the schema
 columns = [i for i in song_dict.keys()]

 song_df = pd.DataFrame(song_dict, columns=columns)

# Filter the desired data
 yesterday_songs = song_df[song_df["timestamp"]==f'{yesterday_formatted}']

# Store the df data inside a table from the current DB
 yesterday_songs.to_sql('tunes', con=engine, index=False, if_exists='append')
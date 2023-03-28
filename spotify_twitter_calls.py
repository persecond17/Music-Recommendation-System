import sys
import os
import io
import json
import requests
import pandas as pd
import numpy as np
import time
from datetime import date, timedelta
import nltk
from nltk.corpus import stopwords

import tweepy
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
from user_definition_st import *

from google.cloud import storage

os.environ["no_proxy"] = "*"


# Spotify API return music lists
def init_set_sp(cid, secret) -> Spotify:
    """
    Establish a connection to the Spotify API.
    """
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp


def get_song_metric(sp, song_id, features_list) -> list:
    """
    Retrieves specific features for a song from
    Spotify given a song ID and a list of features.
    """
    mec = sp.audio_features(song_id)[0]
    line = list(pd.DataFrame(mec, index=[0])[features_list].iloc[0, :])
    return line


def get_play_list(cid, secret, playlist_ids, features_list,
                  whole_file_path, new_file_path, bucket_name,
                  service_account_key_file) -> object:
    """
    Parsing features of songs in selected playlists,
    assigning a day marker (day_of_year) to each song,
    and saving them to the whole.csv stored on Google Cloud Platform.
    Saving any added new songs of the selected playlists to a separate
    f"new_{date.today()}.csv" file and updates whole.csv to
    include them, allowing for the continuous expansion of the data.
    """
    column_names = ["name", "id", "artist", "popularity", "add_day"] + features_list
    df = pd.DataFrame({name: [] for name in column_names})
    try:
        df = read_csv_from_gcs(bucket_name, whole_file_path, service_account_key_file)
    except:
        write_csv_to_gcs(bucket_name, whole_file_path, service_account_key_file, df)

    sp = init_set_sp(cid, secret)
    name_list = list(df.name)
    day_of_year = int(time.strftime("%j", time.gmtime()))
    df_list = []
    for playlist_id in playlist_ids:
        json_file = sp.playlist(playlist_id, fields=None, market=None, additional_types=("track",))
        for j in range(len(json_file["tracks"]["items"])):
            track = json_file["tracks"]["items"][j]
            namex = track["track"]["name"]
            popularity = track["track"]["popularity"]
            if (namex not in name_list) and (int(popularity) >= 60):  # Filter songs by popularity
                name_list.append(namex)
                idx = track["track"]["id"]
                line = get_song_metric(sp, idx, features_list)
                artist = ",".join([track["track"]["artists"][i]["name"]
                                   for i in range(len(track["track"]["artists"]))])
                df_list.append([namex, idx, artist, popularity, day_of_year] + line)

    res = pd.DataFrame(df_list, columns=column_names)
    whole = pd.concat([df, res], ignore_index=True)
    write_csv_to_gcs(bucket_name, whole_file_path, service_account_key_file, whole)
    write_csv_to_gcs(bucket_name, new_file_path, service_account_key_file, res)
    return whole, res


def to_twipy(cid, secret, playlist_ids, features_list, whole_file_path,
             new_file_path, bucket_name, service_account_key_file) -> list[list]:
    """
    Evenly distribute the songs from the 'whole.csv' file across the
    collection period, combine any new songs that are added the day.
    Extract the 'name' and 'artist' of each song to send to Twitter
    API for tweets collection.
    """
    df_whole, df_new = get_play_list(cid, secret, playlist_ids, features_list,
                                     whole_file_path, new_file_path, bucket_name,
                                     service_account_key_file)

    # Plan A: normally collecting within collection period
    first_day = sorted(df_whole.add_day.unique())[0]
    today = time.strftime("%j", time.gmtime())
    i = int(today) - int(first_day)
    epoch = 7
    step = len(df_whole[df_whole.add_day == first_day]) // epoch
    selected = [day for day in range(i * step, (i + 1) * step)]

    # Plan B: Specify collecting manually in case of normally collecting broken or over
    # selected = range(1500, 1800) # specify the collecting range
    # selected = []  # just retrieve all new songs

    df0 = df_whole[['name', 'artist']].iloc[selected, :]
    if df_new.shape[0] != 0:
        res = pd.concat([df0, df_new[['name', 'artist']]], ignore_index=True)
    else:
        res = df0
    return res.values.tolist()


# Twitter API return tweets
def authenticate(consumer_key, consumer_secret, access_token, access_token_secret) -> object:
    """
    Access the Twitter API.
    """
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twi_api = tweepy.API(auth, wait_on_rate_limit=True)
    return twi_api


def fetch_status(status) -> dict:
    """
    Parsing the JSON format of tweets returned by the Twitter API.
    """
    status_info = {'tweet_id': str(status.id),  # convert to string so csv doesn't convert to exponential
                   'created': status.created_at,
                   'retweeted': status.retweet_count,
                   'content': status.full_text,
                   'hashtags': [hashtag['text'] for hashtag in status.entities['hashtags']],
                   'urls': status.entities['urls'],
                   'user_id': str(status.user.id),  # convert to string so csv doesn't convert to exponential
                   'username': status.user.screen_name,
                   'location': status.user.location,
                   'num_followers': status.user.followers_count,
                   'geo_enabled': status.user.geo_enabled}
    if status.coordinates:
        status_info['long'] = status.coordinates['coordinates'][0]
        status_info['lat'] = status.coordinates['coordinates'][1]
    else:
        status_info['long'] = None
        status_info['lat'] = None
    return status_info


def search_tweets(api, query, end_date) -> list[dict]:
    """
    Retrieve up to the first 5000 non-retweeted tweets for each query.
    """
    searched_tweets = [fetch_status(status) for status in tweepy.Cursor(api.search_tweets,
                                                                        q=query,
                                                                        until=end_date,
                                                                        lang='en',
                                                                        tweet_mode='extended'
                                                                        ).items(5000)
                       if (not fetch_status(status)['retweeted'])]
    return searched_tweets


def check_artists(song, artist) -> str:
    """
    For songs with multiple artists, split the names and extract
    the first name of each artist. If the first name is not a stop word,
    use it as the artist representation. Otherwise, use the full name.
    Combine the song name and each artist representation using 'OR'
    logical operators into a single string.
    """
    stop_words = set(stopwords.words('english'))
    artists = artist.split(',')

    # multiple artists
    if len(artists) > 1:
        temp = []
        for s in range(len(artists)):
            name = artists[s].split(' ')
            if len(name) > 1 and name[0].lower() not in stop_words:
                temp.append(f'("{song}" AND "{name[0]}")')
            else:
                temp.append(f'("{song}" AND "{artists[s]}")')
        query = ' OR '.join(temp)

    # single artist
    else:
        name = artists[0].split(' ')
        if len(name) > 1 and name[0].lower() not in stop_words:
            query = f'("{song}" AND "{name[0]}")'  # only query last name
        else:
            query = f'("{song}" AND "{artists[0]}")'  # query the full name
    return query


def query_in_df(song_list, api, end_date, bucket_name, service_account_key_file):
    """
    Create a new directory to store daily tweet collections.
    Reformat song names by retrieving only the portion before
    any special characters. Save each query result to a file
    named '{end_date}/{song}.csv' on GCP.
    """
    create_dir(bucket_name, end_date)
    for song, artist in song_list:
        # reformat song names
        song_q = song
        for i in range(1, len(song)):
            if song[i] == '-' or song[i] == '(':
                song_q = song[:i - 1]
                break

        # build query for Twitter API
        query = check_artists(song_q, artist)
        print(query)
        time.sleep(1)
        results = search_tweets(api=api, query=query, end_date=end_date)
        df_mini = pd.DataFrame.from_records(results)

        # save the result to a csv file on GCP
        blob_name = f"{end_date}/{song}.csv"
        write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df_mini)


def create_dir(bucket_name, end_date):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    # Check if the directory already exists
    if len(list(bucket.list_blobs(prefix=end_date))) == 0:
        blob = bucket.blob(end_date + '/')
        blob.upload_from_string('')


def read_csv_from_gcs(bucket_name, blob_name, service_account_key_file) -> pd.core.frame.DataFrame:
    """
    Read csv file from Google Cloud Storage.
    """
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)  # path to the file

    file_string = blob.download_as_string().decode('utf-8')
    df = pd.read_csv(io.StringIO(file_string))
    return df


def write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df):
    """
    Write and read a blob from GCS using file-like IO.
    """
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        df.to_csv(f, index=False)


def main(cid, secret, playlist_ids, features_list, whole_file_path,
         new_file_path, bucket_name, service_account_key_file, end_date,
         consumer_key, consumer_secret, access_token, access_token_secret):
    song_list = to_twipy(cid, secret, playlist_ids, features_list, whole_file_path,
                         new_file_path, bucket_name, service_account_key_file)
    api = authenticate(consumer_key, consumer_secret, access_token, access_token_secret)
    return query_in_df(song_list, api, end_date, bucket_name, service_account_key_file)


# If you need to run the program locally:
# if __name__ == '__main__':
#     main(cid, secret, playlist_ids, features_list, whole_file_path,
#          new_file_path, bucket_name, service_account_key_file, end_date,
#          consumer_key, consumer_secret, access_token, access_token_secret)


# If you need to deploy this program on Airflow:
# import this python file into airflow_st.py
from datetime import date, datetime, timedelta
import os

cid = "Your_cid_for_Spotify_API"
secret = "Your_secret_for_Spotify_API"
whole_file_path = "whole.csv"
new_file_path = f"new_{date.today()}.csv"
features_list = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'duration_ms', 'time_signature']
playlist_ids = ['37i9dQZF1DXcBWIGoYBM5M',
                '37i9dQZF1DX2L0iB23Enbq',
                '5TDtuKDbOhrfW7C58XnriZ',
                '37i9dQZF1EQncLwOalG3K7',
                '37i9dQZF1DWUa8ZRTfalHk',
                '37i9dQZEVXbLiRSasKsNU9',
                '2HL9uBibWwlRROmOvjK5tA',
                '31ymdYCITDnZRtkKzP3Itp',
                '0SRdjSDz4RsuozwHv1zhvr',
                '37i9dQZEVXbMDoHDwVN2tF',
                '5ABHKGoOzxkaa28ttQV9sE',
                '6UeSakyzhiEt4NB3UAd6NQ',
                '37i9dQZF1DX4JAvHpjipBk',
                '37i9dQZF1DX9lzz0FRAxgl',
                '37i9dQZF1DX4dyzvuaRJ0n',
                '37i9dQZF1DX76Wlfdnj7AP',
                '37i9dQZF1DX5gQonLbZD9s',
                '37i9dQZF1DX6VdMW310YC7',
                '37i9dQZF1DX5Vy6DFOcx00',
                '37i9dQZF1DX9wC1KY45plY',
                '37i9dQZF1DWV7EzJMK2FUI',
                '37i9dQZF1DX6GwdWRQMQpq',
                '37i9dQZF1DX7gIoKXt0gmx',
                '37i9dQZF1DXcF6B6QPhFDv',
                '37i9dQZF1DWSqmBTGDYngZ',
                '37i9dQZF1DX3rxVfibe1L0',
                '37i9dQZF1DX4OzrY981I1W',
                '37i9dQZF1DWTl4y3vgJOXW',
                '37i9dQZF1DWWvvyNmW9V9a',
                '37i9dQZF1DX4JAvHpjipBk',
                '37i9dQZF1DWWwaxRea1LWS',
                '37i9dQZF1DX8ky12eWIvcW',
                '37i9dQZF1DX4pUKG1kS0Ac',
                '37i9dQZF1DX0s5kDXi1oC5']

consumer_key = "Your_consumer_key_for_Twitter_API"
consumer_secret = "Your_consumer_secret_for_Twitter_API"
access_token = "Your_access_token_for_Twitter_API"
access_token_secret = "Your_access_token_secret_for_Twitter_API"

end_date = str(date.today())

# Set configurations to connect to Google Cloud Bucket and MongoDB
bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
mongo_username = os.environ.get("MONGO_USERNAME")
mongo_password = os.environ.get("MONGO_PASSWORD")
mongo_ip_address = os.environ.get("MONGO_IP")
database_name = os.environ.get("MONGO_DB_NAME")
collection_name = os.environ.get("MONGO_COLLECTION_NAME")

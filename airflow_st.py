import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from user_definition_st import *
from spotify_twitter_calls import *


def pull_songs_and_tweets(**kwargs):
    """
    Trigger the twitter collecting process,
    and query songs with Spotify API.
    """
    song_list = to_twipy(cid, secret, playlist_ids, features_list, whole_file_path,
                         new_file_path, bucket_name, service_account_key_file)
    api = authenticate(consumer_key, consumer_secret, access_token, access_token_secret)
    return query_in_df(song_list, api, end_date, bucket_name, service_account_key_file)


with DAG(dag_id='spotify_twitter',
         start_date=datetime(2023, 2, 23),
         catchup=False,
         schedule_interval='0 10 * * *') as dag:  # run it every 10am

         
    pull_songs_and_tweets = PythonOperator(task_id="pull_songs_and_tweets",
                                           python_callable=pull_songs_and_tweets,
                                           op_kwargs={'cid': cid,
                                                      'secret': secret,
                                                      'playlist_ids': playlist_ids,
                                                      'features_list': features_list,
                                                      'bucket_name': bucket_name,
                                                      'service_account_key_file': service_account_key_file,
                                                      'whole_file_path': whole_file_path,
                                                      'new_file_path': new_file_path,
                                                      'consumer_key': consumer_key,
                                                      'consumer_secret': consumer_secret,
                                                      'access_token': access_token,
                                                      'access_token_secret': access_token_secret,
                                                      'end_date': end_date})

    pull_songs_and_tweets

from datetime import datetime
from google.cloud import storage
from numpy.linalg import norm
from bs4 import BeautifulSoup
import numpy as np
import itertools
import pymongo
import pyspark
import re
import string
import requests
import random
import json
import time
import os

from scipy.stats import ttest_1samp, ttest_ind
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, MinMaxScaler, MaxAbsScaler, Normalizer, HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors, DenseVector, VectorUDT

import json
import nltk
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer, SnowballStemmer, PorterStemmer

import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
stop_words = set(stopwords.words('english'))

import warnings 
warnings.filterwarnings(action = 'ignore') 

def tfidf_model(df, numFeatures=100000):
    """
    Train a TF model and a IDF model.
    Transform the input using tf_md and idf_md.
    """
    # tf
    input_name=df.columns[1]
    tf_md = HashingTF(inputCol=input_name, outputCol="rawFeatures", numFeatures=numFeatures)
    featurizedData = tf_md.transform(df)

    # idf
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_md = idf.fit(featurizedData)
    rescaledData = idf_md.transform(featurizedData)
    return tf_md, idf_md, rescaledData


def converting_input(input_text: str) -> DataFrame:
    """
    Split input_text into words.
    Duplicate input until it has at least 15 
    words for better model identification.
    """
    input = tokenize(input_text)
    while len(input_text) < 15:
        input *= 5
    df_input = spark.createDataFrame([[input]],["raw"])
    return df_input


def tokenize(text: str) -> list:
    """
    Normalize each word to lowercase, strip punctuation,
    remove stop words, drop words of length <= 2, strip digits.
    Stem text and return a list of tokenized words.
    """
    text = text.lower()
    text = re.sub('[' + string.punctuation + '0-9\\r\\t\\n]', ' ', str(text))
    tokens = text.split(' ')
    tokens = [w for w in tokens if len(w) > 2]  # ignore a, an, to, at, be, ...
    tokens = [w for w in tokens if w not in stop_words]
    # tokens = [stemming(token) for token in tokens]
    return tokens


def tfidf_recommendation_3(input_text, tf_md, idf_md, rescaledData, recommend_num,df_features):
    start_time = time.time()
    input_dataframe= converting_input(input_text)
    input_vec =  tf_md.transform(input_dataframe).head().rawFeatures
    udf_sv_product=udf(lambda x: float(input_vec.dot(x)),FloatType())
    rescaledData_pd = rescaledData.select('id',udf_sv_product(rescaledData.features).alias('dot_pd')).toPandas()
    res_id=list(rescaledData_pd.nlargest(recommend_num, columns='dot_pd')['id'].to_numpy())

    # res_id = [x.asDict()['id'] for x in rescaledData_pd.sort(rescaledData_pd.dot_pd.desc()).select('id').head(recommend_num)]
    print(f'This recommendation spent {time.time()-start_time:.1f} seconds.')
    return df_features.filter(col('_id').isin(res_id)).select('name')


def recommend_system(input, file_df_path, recommend_num):
    # Load_files
    df = spark.read.load(file_df_path[0])
    df_features = spark.read.load(file_df_path[1])

    # Fit tfidf models
    tf_md, idf_md, rescaledData = tfidf_model(df)
    res = tfidf_recommendation_3(input, tf_md, idf_md, rescaledData, recommend_num, df_features)

    print(res)


spark = SparkSession.builder.config("spark.network.timeout", "360000000s")\
                    .config("spark.executor.heartbeatInterval", "3600s")\
                    .getOrCreate()

file_path = 'models/'
file_df_path = [file_path+'lyirc_tweet_corpus_split_df', file_path+'df_features']
input_text = input("The input sentence: ")
recommend_num = input("The number of songs you want: ")
recommend_system(input_text, file_df_path, recommend_num)

import pymongo
import pandas as pd
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime
import time
import sys

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.Twitter
tweets = db.tweets_data

all_tweets=[]
if __name__ == "__main__":
    tweets_df = pd.io.json.json_normalize(tweets.find({}).limit(10000), max_level=2)
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(tweets_df, f, ensure_ascii=False, indent=4)
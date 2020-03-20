import pymongo
import csv
import pandas as pd
import json

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.Twitter
tweets = db.tweets_data.find()
tweets_d = db.tweets_data

hashtags = []

if __name__ == "__main__":

    print("=== Getting Hashtags ===")
    for tweet in tweets:
        try:
            tweet_hashtags = ""
            for tag in tweet['entities']['hashtags']:
                if tweet_hashtags == "":
                    tweet_hashtags = tag['text']
                else:
                    tweet_hashtags = tweet_hashtags + " " + tag['text']
            if tweet_hashtags != "":
                hashtags.append(tweet_hashtags)
        except:
            pass

    tweets_df = pd.io.json.json_normalize(tweets_d.find({}).limit(10000), max_level=2)

    print(tweets_d['retweet'])

    print("=== Writing CSV File ===")

    with open('hashtags.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['hashtags'])
        for entry in hashtags:
            writer.writerow([entry])

    print("=== Done ===")

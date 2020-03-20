import pymongo
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import adjusted_rand_score
import re
from collections import Counter

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.Twitter
tweets = db.tweets_data

documents = []

username_data = []
hashtag_data = []


if __name__ == "__main__":
    print("=== Retrieving data from database ===")
    for tweet in tweets.find():
        try:
            if tweet['truncated'] == True:
                documents.append(tweet['extended_tweet']['full_text'])
            else:
                documents.append(tweet['text'])
        except:
            pass

    print("=== Vectorising ===")
    vectorizer = TfidfVectorizer()
    transform = vectorizer.fit_transform(documents)

    true_k = 15

    model = KMeans(n_clusters=true_k, init='random', max_iter=150, n_init=15)
    model.fit(transform)
    
    order_centroids = model.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names()
    clusters = {}
    print(model.labels_)
    for i in model.labels_:
        if not i in clusters:
            clusters[i] = 1
        else:
            clusters[i] += 1

    with open("clustering.txt", "w+") as f:
        f.write("Top terms per cluster:\n")
    for i in range(true_k):
        with open("clustering.txt", "a") as f:
            f.write(f"Cluster {i}\n")
            f.write(f"Number of tweets in this cluster: {clusters[i]}\n")

    
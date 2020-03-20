import pandas as pd
import numpy as np
from scipy import stats
from operator import itemgetter
import re
import json
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.Twitter
tweets = db.tweets_data

# User info
def get_info(tweets_import):
    print("=== Info ===")
    tweets_import["screen_name"] = tweets_df['user.screen_name']
    tweets_import["user_id"] = tweets_df["user.id"]
    tweets_import["followers_count"] = tweets_df["user.followers_count"]
    return tweets_import

# Retweets
def get_retweets(tweets_import):
    print("=== Retweets ===")
    tweets_import["retweeted_screen_name"] = tweets_df["retweeted_status.user.screen_name"]
    tweets_import["retweeted_id"] = tweets_df["retweeted_status.user.id_str"]
    return tweets_import

# Quoted tweets
def get_quoted(tweets_import):
    print("=== Quoted Tweets ===")
    tweets_import["quoted_screen_name"] = tweets_df["quoted_status.user.screen_name"]
    tweets_import["quoted_id"] = tweets_df["quoted_status.user.id_str"]
    return tweets_import

# User mentions
def get_usermentions(tweets_import):
    print("=== User Mentions ===")
    if not tweets_df['entities.user_mentions'].empty:
        tweets_import["user_mentions_screen_name"] = tweets_df['entities.user_mentions'][0][0]['screen_name']
        tweets_import["user_mentions_id"] = tweets_df['entities.user_mentions'][0][0]["id_str"]
    return tweets_import


# User interactions
def get_interactions(row):
    user = row["user_id"], row["screen_name"]

    if user[0] is None:
        return (None, None), []

    inter = set()

    # Replies
    #inter.add((row["in_reply_to_user_id"], row["in_reply_to_screen_name"]))
    # Quotes
    #inter.add((row["quoted_id"], row["quoted_screen_name"]))
    # Retweets
    #inter.add((row["retweeted_id"], row["retweeted_screen_name"]))
    # Mentions
    inter.add((row["user_mentions_id"], row["user_mentions_screen_name"]))

    inter.discard((row["user_id"], row["screen_name"]))
    # Discard all none values
    inter.discard((None, None))

    return user, inter

# Information about replies
def get_in_reply(tweets_import):
    # Just copy the 'in_reply' columns to the new dataimport
    tweets_import["in_reply_to_screen_name"] = tweets_df["in_reply_to_screen_name"]
    tweets_import["in_reply_to_status_id"] = tweets_df["in_reply_to_status_id"]
    tweets_import["in_reply_to_user_id"]= tweets_df["in_reply_to_user_id"]
    return tweets_import


def fill_df(tweets_import):
    tweets_import = get_info(tweets_import)
    tweets_import = get_usermentions(tweets_import)
    tweets_import = get_retweets(tweets_import)
    tweets_import = get_in_reply(tweets_import)
    tweets_import = get_quoted(tweets_import)
    return tweets_import



if __name__ == "__main__":
    pd.set_option('display.float_format', lambda x: '%.f' % x)
    print("=== Collecting data and creating dataframe ===")
    tweets_df = pd.io.json.json_normalize(tweets.find({}).limit(10000), max_level=2)

    tweets_import = pd.DataFrame(
        columns=["created_at", "id", "in_reply_to_screen_name", "in_reply_to_status_id", "in_reply_to_user_id",
                 "retweeted_id", "retweeted_screen_name", "user_mentions_screen_name", "user_mentions_id",
                 "text", "user_id", "screen_name", "followers_count"])

    # Columns that are going to be the same
    equal_columns = ["created_at", "id", "text"]
    tweets_import[equal_columns] = tweets_df[equal_columns]

    tweets_import = fill_df(tweets_import)

    tweets_import = tweets_import.where((pd.notnull(tweets_import)), None)

    graph = nx.Graph()

    print("=== Interactions ===")

    for index, tweet in tweets_import.iterrows():
        user, interactions = get_interactions(tweet)
        user_id, user_name = user
        tweet_id = tweet["id"]
        # tweet_sent = tweet["sentiment"]
        for interaction in interactions:
            int_id, int_name = interaction
            graph.add_edge(user_id, int_id, tweet_id=tweet_id)

            graph.nodes()[user_id]["name"] = user_name
            graph.nodes()[int_id]["name"] = int_name

    print("=== Graph Data ===")

    print(f"The graph has {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")

    degrees = [val for (node, val) in graph.degree()]
    print(f"Graph's min degree: {np.min(degrees)}")
    print(f"Graph's max degree: {np.max(degrees)}")
    print(f"Average node's degree: {np.mean(degrees):.1f}")
    print(f"Most frequent degree of the nodes: {stats.mode(degrees)[0][0]}")

    if nx.is_connected(graph):
        print("The graph is connected")
    else:
        print("The graph is not connected")

    print(f"There are {nx.number_connected_components(graph)} connected components in the Graph")

    print("=== Graph ===")

    pos = nx.spring_layout(graph, k=0.10)
    plt.figure()
    #nx.draw(graph, pos=pos, edge_color="black", linewidths=0.03,node_size=15, alpha=0.6, with_labels=False)
    nx.draw_networkx_nodes(graph, pos=pos, node_size=20, node_color=range(graph.number_of_nodes()), cmap="coolwarm",)
    #plt.savefig('graph2.png')
    plt.show()

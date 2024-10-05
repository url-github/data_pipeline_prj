import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

load_dotenv()

def run_twitter_etl():
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    authentication_token = os.getenv("AUTHENTICATION_TOKEN")
    authentication_secret = os.getenv("AUTHENTICATION_SECRET")

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(authentication_token, authentication_secret)

    api = tweepy.API(auth)

    try:
        tweets = api.search_tweets(q='from:sikorskiradek', count=200, tweet_mode='extended')
        print(f"Znaleziono {len(tweets)} tweetów.")
    except Exception as e:
        print(f"Nie udało się pobrać tweetów: {e}")
        return

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)

    if df.empty:
        print("Brak danych do zapisania w pliku CSV.")
    else:
        df.to_csv('refined_tweets.csv', index=False)
        print("Zapisano dane do pliku refined_tweets.csv")

if __name__ == "__main__":
    run_twitter_etl()
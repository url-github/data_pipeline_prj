import tweepy
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def run_favorites():
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    authentication_token = os.getenv("AUTHENTICATION_TOKEN")
    authentication_secret = os.getenv("AUTHENTICATION_SECRET")

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(authentication_token, authentication_secret)

    api = tweepy.API(auth)

    try:
        tweets = api.favorites(count=10, tweet_mode='extended')
        print(f"Znaleziono {len(tweets)} ulubionych tweetów.")
    except Exception as e:
        print(f"Nie udało się pobrać tweetów: {e}")
        return

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]
        refined_tweet = {
            "user": tweet.user.screen_name,
            'text': text,
            'favorite_count': tweet.favorite_count,
            'retweet_count': tweet.retweet_count,
            'created_at': tweet.created_at
        }
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('favorites_tweets.csv', index=False)
    print("Zapisano dane do pliku favorites_tweets.csv")

if __name__ == "__main__":
    run_favorites()
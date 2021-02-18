from dagster import pipeline

from lena_tweets.solids import (
    get_friends_of_users,
    get_ids_collect_info,
    collect_tweets_of_users,
)


@pipeline
def kick_off_study():
    get_ids_collect_info()


@pipeline
def tweet_history():
    collect_tweets_of_users()


@pipeline
def daily_user_scrape():
    get_friends_of_users()


@pipeline
def daily_tweet_scrape():
    collect_tweets_of_users()

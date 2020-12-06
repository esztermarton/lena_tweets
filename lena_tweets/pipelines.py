from dagster import execute_pipeline, pipeline

from lena_tweets.solids import (
    collect_user_information, 
    collect_tweets_they_see, 
    read_in_user, 
    get_friends_of_user,
    get_ids_collect_info,
    lookup_users_daily, 
    collect_tweets_of_user,
)


@pipeline
def daily_twitter_scrape():
    people_that_tweet = collect_user_information()
    collect_tweets_they_see(people_that_tweet)

@pipeline
def kick_off_study():
    get_ids_collect_info()

@pipeline
def daily_user_scrape():
    handle = read_in_user()
    friend_ids = get_friends_of_user(handle)
    lookup_users_daily(friend_ids)

@pipeline
def daily_tweet_scrape():
    collect_tweets_of_user()
from datetime import datetime

from dagster import repository, daily_schedule

from lena_tweets.config import TIMESTAMP_FORMAT
from lena_tweets.pipelines import (
    daily_twitter_scrape,
    daily_user_scrape,
    daily_tweet_scrape,
    kick_off_study,
)


@daily_schedule(pipeline_name="daily_twitter_scrape", start_date=datetime(2020, 11, 26))
def my_daily_schedule(date):
    return {
        "solids": {
            "collect_user_information": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)}
            },
            "collect_tweets_they_see": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)}
            },
        }
    }


@repository(name="lena_tweets")
def repo():
    return [daily_twitter_scrape, my_daily_schedule, daily_user_scrape, daily_tweet_scrape, kick_off_study]

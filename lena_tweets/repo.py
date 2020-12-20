from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import repository

from lena_tweets.config import TIMESTAMP_FORMAT, PARTICIPANTS_QUEUE, USER_TRACKER_PATH
from lena_tweets.partition_schedule import minute_schedule
from lena_tweets.pipelines import (
    daily_user_scrape,
    daily_tweet_scrape,
    kick_off_study,
    tweet_history,
)

today_day = datetime.now().day

def queue_people(_):
    """Returns whether people are left in the queue"""
    today = datetime.now().strftime(TIMESTAMP_FORMAT)
    today_file = PARTICIPANTS_QUEUE.format(today)
    with open(today_file, "r") as f:
        user_ids = f.readlines()

    return bool(user_ids)


def outstanding_tweet_history(_):
    if not Path(USER_TRACKER_PATH).exists():
        return False

    df = pd.read_csv(USER_TRACKER_PATH)
    df_new = df[df["tweets_last_retrieved"].isna()]

    return bool(len(df_new))


@minute_schedule(pipeline_name="daily_user_scrape", start_date=datetime(2020, 12, today_day), should_execute=queue_people)
def my_minute_schedule(date):
    return {
        "solids": {
            "get_friends_of_user": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)},
            },
        }
    }

@minute_schedule(pipeline_name="daily_tweet_scrape", start_date=datetime(2020, 12, today_day))
def my_minute_schedule_tweet(date):
    return {
        "solids": {
            "collect_tweets_of_users": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)},
            },
        }
    }


@minute_schedule(pipeline_name="tweet_history", start_date=datetime(2020, 12, today_day), should_execute=outstanding_tweet_history)
def my_minute_schedule_tweet_history(date):
    return {
        "solids": {
            "collect_tweets_of_users": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)},
                "inputs": {"all_tweets": True},
            },
        }
    }


@repository(name="lena_tweets")
def repo():
    return [daily_user_scrape, my_minute_schedule, daily_tweet_scrape, my_minute_schedule_tweet, kick_off_study, tweet_history, my_minute_schedule_tweet_history]

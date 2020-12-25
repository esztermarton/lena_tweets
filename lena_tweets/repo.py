from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import repository

from lena_tweets.config import TIMESTAMP_FORMAT
from lena_tweets.database import connection_manager, Tracker
from lena_tweets.partition_schedule import minute_schedule
from lena_tweets.pipelines import (
    daily_user_scrape,
    daily_tweet_scrape,
    kick_off_study,
    tweet_history,
)

today_day = datetime.now().day


@connection_manager()
def queue_people(_):
    """Returns whether people are left that haven't yet been checked"""
    today = datetime.now()
    today_date = datetime(today.year, today.month, today.day)
    not_checked_today = Tracker.select().where(
        (
            (Tracker.friends_last_retrieved.is_null())
            | (Tracker.friends_last_retrieved < today_date)
        )
        & (Tracker.participant == True)
    )
    return not_checked_today.count() > 0


@connection_manager()
def outstanding_tweet_history(_):
    return bool(Tracker.select().where(Tracker.latest_tweet_id.is_null()).count())


@minute_schedule(
    pipeline_name="daily_user_scrape",
    start_date=datetime(2020, 12, today_day),
    cron_schedule="*/3 * * * *",
    should_execute=queue_people,
)
def my_three_minute_schedule(date):
    return {
        "solids": {
            "get_friends_of_user": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)},
            },
        }
    }


@minute_schedule(
    pipeline_name="daily_tweet_scrape",
    cron_schedule="*/3 * * * *",
    start_date=datetime(2020, 12, today_day),
)
def my_three_minute_schedule_tweet(date):
    return {
        "solids": {
            "collect_tweets_of_users": {
                "config": {"timestamp": date.strftime(TIMESTAMP_FORMAT)},
            },
        }
    }


@minute_schedule(
    pipeline_name="tweet_history",
    start_date=datetime(2020, 12, today_day),
    cron_schedule="*/3 * * * *",
    should_execute=outstanding_tweet_history,
)
def my_three_minute_schedule_tweet_history(date):
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
    return [
        daily_user_scrape,
        my_three_minute_schedule,
        daily_tweet_scrape,
        my_three_minute_schedule_tweet,
        kick_off_study,
        tweet_history,
        my_three_minute_schedule_tweet_history,
    ]

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
import tweepy
from dagster import solid
from tweepy import User, Status

from lena_tweets.config import (
    TIMESTAMP_FORMAT,
    DAILY_FRIENDS_CHECK_PATH,
    DAILY_TWEETS_PATH,
    STUDY_END_PATH,
    STUDY_START_PATH,
    STUDY_INPUT_START_PART,
    TWEET_HISTORY,
)
from lena_tweets.database import connection_manager, Tracker
from lena_tweets.scrape_twitter import (
    get_friends,
    get_user_tweets,
    get_friends_ids,
    lookup_users,
    get_all_most_recent_tweets,
)


@solid
def get_ids_collect_info(context):
    """
    Converts a file of screen names to user ids & collects study start info
    """

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    study_start_path = STUDY_START_PATH.format(timestamp)

    with open(STUDY_INPUT_START_PART) as f:
        screen_names = [f.strip() for f in f.readlines() if f.strip()]

    if Path(study_start_path).exists():
        screen_names_already_seen = set(
            pd.read_csv(study_start_path, lineterminator="\n")["screen_name"]
        )
        context.log.info(
            f"{study_start_path} already exists, {len(screen_names_already_seen)} already exist."
        )
        header = False
    else:
        screen_names_already_seen = set()
        context.log.info(f"{study_start_path} doesn't exist yet")
        header = True

    for screen_name in screen_names:
        if screen_name in screen_names_already_seen:
            context.log.debug("{screen_name} already seen")
            continue
        try:
            user, friends = get_friends(context.log, screen_name)
        except tweepy.RateLimitError as exc:
            context.log.error(
                "Rate limit reached. Sleeping for the next round 3 minutes"
            )
            minute = datetime.now().minute
            while datetime.now().minute == minute or datetime.now().minute % 3 != 0:
                time.sleep(5)
            user, friends = get_friends(context.log, screen_name)
        except tweepy.error.TweepError as exc:
            context.log.error(str(exc))
            continue
        context.log.info(f"Got id and friends of user {screen_name}")
        users = [user] + friends
        new_users = []
        for i, u in enumerate(users):
            if not u.screen_name in screen_names_already_seen:
                new_users.append(u)
                screen_names_already_seen.add(u.screen_name)

            # First element in list is the participant themselves
            _add_to_tracker(u.id, participant=(i == 0))

        df = _convert_friends_to_dataframe(new_users)
        df.to_csv(study_start_path, index=False, mode="a", header=header)


@connection_manager()
def _add_to_tracker(user_id: int, participant: bool = False):
    user, _ = Tracker.get_or_create(user_id=user_id)
    if participant:
        user.participant = True
        user.friends_last_retrieved = datetime.now()
        user.save()


@connection_manager()
def _get_next_user() -> int:
    never_checked = Tracker.select().where(
        (Tracker.friends_last_retrieved.is_null()) & (Tracker.participant == True)
    )
    if never_checked.count():
        return never_checked.first().user_id
    return (
        Tracker.select()
        .where(Tracker.participant == True)
        .orderby(Tracker.friends_last_retrieved)
        .first()
        .user_id
    )


@solid(config_schema={"timestamp": str})
def get_friends_of_users(context):
    for _ in range(10):
        try:
            get_friends_of_user(context)
        except tweepy.RateLimitError as exc:
            context.log.error("tweepy.RateLimitError, will continue from here.")
            break


def get_friends_of_user(context):
    """
    Collects friends of a user and appends it to a csv file.
    """
    next_user_id = _get_next_user()

    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )
    friends_ids = get_friends_ids(context.log, next_user_id)

    header = not Path(DAILY_FRIENDS_CHECK_PATH.format(timestamp)).exists()

    pd.DataFrame({"user_id": next_user_id, "friends_id": friends_ids}).to_csv(
        DAILY_FRIENDS_CHECK_PATH.format(timestamp), mode="a", header=header, index=False
    )
    for u in friends_ids:
        _add_to_tracker(u)

    _add_to_tracker(next_user_id, participant=True)


@solid(config_schema={"timestamp": str})
def collect_tweets_of_users(context, all_tweets: bool = False):
    """
    Collects tweets the user tweets
    """
    initial_timestamp = datetime.now()
    while datetime.now() - initial_timestamp < timedelta(minutes=3):
        try:
            collect_tweets_of_user(context, all_tweets=all_tweets)
        except tweepy.RateLimitError as exc:
            context.log.error("tweepy.RateLimitError, will continue from here.")
            break

    context.log.info("Have been running for over 3 minutes, returning")
    return


@connection_manager()
def _get_next_user_for_tweets():
    never_checked = Tracker.select().where(Tracker.tweets_last_retrieved.is_null())
    if never_checked.count():
        return never_checked.first()
    return Tracker.select().orderby(Tracker.tweets_last_retrieved).first()


@connection_manager()
def _update_item(item, latest_tweet_id):
    item.tweets_last_retrieved = datetime.now()
    item.latest_tweet_id = latest_tweet_id
    item.save()


def collect_tweets_of_user(context, all_tweets: bool = False):
    """
    Collects tweets the user tweets
    """
    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )

    next_item = _get_next_user_for_tweets()
    user_id = next_item.user_id
    latest_tweet_id = next_item.latest_tweet_id

    if all_tweets:
        statuses = _convert_tweets_to_dataframe(
            user_id, get_all_most_recent_tweets(context.log, user_id)
        )
        tweet_file_path = Path(TWEET_HISTORY)

    else:
        statuses = _convert_tweets_to_dataframe(
            user_id, get_user_tweets(context.log, user_id, since_id=latest_tweet_id)
        )
        tweet_file_path = Path(DAILY_TWEETS_PATH.format(timestamp))

    header = not tweet_file_path.exists()
    statuses.to_csv(tweet_file_path, mode="a", header=header, index=False)

    context.log.info(f"Collected {len(statuses)} tweets for user {user_id}")

    _update_item(next_item, statuses.iloc[0]["id"] if len(statuses) else None)

    context.log.info(f"Updated user_id {user_id}, {len(statuses)} new tweets")


def _convert_friends_to_dataframe(users: List[User]):
    return pd.DataFrame(
        [
            {
                "user_id": user.id,
                "screen_name": user.screen_name,
                "name": user.name,
                "description": user.description,
            }
            for user in users
        ]
    )


def _convert_tweets_to_dataframe(user_id: int, tweets: List[Status]):
    return pd.DataFrame(
        [
            {
                "user_id": int(float(user_id)),
                "id": int(float(tweet.id)),
                "text": tweet.text,
                "created_at": tweet.created_at,
            }
            for tweet in tweets
        ]
    )

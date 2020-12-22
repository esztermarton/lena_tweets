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
    PARTICIPANTS_QUEUE,
    USER_TRACKER_PATH,
    TWEET_HISTORY,
)
from lena_tweets.scrape_twitter import (
    get_friends,
    get_user_tweets,
    get_friends_ids,
    lookup_users,
    get_all_most_recent_tweets,
)


lock_file_a = Path(__file__).parent / ".lockfile_a"
lock_file_b = Path(__file__).parent / ".lockfile_b"


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
        screen_names_already_seen = set(pd.read_csv(study_start_path, lineterminator="\n")["screen_name"])
        context.log.info(f"{study_start_path} already exists, {len(screen_names_already_seen)} already exist.")
        header = False
    else:
        screen_names_already_seen = set()
        context.log.info(f"{study_start_path} doesn't exist yet")
        header = True

    for screen_name in screen_names:
        if screen_name in screen_names_already_seen:
            continue
        try:
            user, friends = get_friends(context.log, screen_name)
        except tweepy.error.TweepError as exc:
            context.log.error(str(exc))
            continue
        context.log.info(f"Got id and friends of user {screen_name}")
        users = [user] + friends
        new_users = []
        for u in users:
            if not u.screen_name in screen_names_already_seen:
                new_users.append(u)
                screen_names_already_seen.add(u.screen_name)

        df = _convert_friends_to_dataframe(new_users)
        df.to_csv(study_start_path, index=False, mode="a", header=header)
    
        tracking_df = df[["user_id"]]
        tracking_df["latest_tweet_id"] = None
        tracking_df["tweets_last_retrieved"] = None
        tracking_df.to_csv(USER_TRACKER_PATH, index=False, mode="a", header=header)

        header = False

        first_today_file = PARTICIPANTS_QUEUE.format(timestamp)
        with open(first_today_file, "a") as f:
            f.write(str(user.id) + "\n")


@solid
def read_in_user(_) -> int:
    """
    Reads in the next participant user from the file for the day.

    Raises:
        ValueError: if no participants are left
    """
    today = datetime.now().strftime(TIMESTAMP_FORMAT)
    tomorrow = (datetime.now() + timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
    today_file = PARTICIPANTS_QUEUE.format(today)
    tomorrow_file = PARTICIPANTS_QUEUE.format(tomorrow)

    with open(today_file, "r") as f:
        user_ids = f.readlines()

    # If empty file, raise value error
    if not user_ids:
        raise ValueError("No more participants to examine today")

    next_user_id, rest = user_ids[0], user_ids[1:]

    # Discard if next_screen_name is just a new line
    while next_user_id == "\n":
        if rest:
            next_user_id, rest = rest[0], rest[1:]
        else:
            with open(today_file, "w") as f:
                f.writelines(rest)
            raise ValueError("No more participants to examine today")

    # Write rest back to file
    with open(today_file, "w") as f:
        f.writelines(rest)

    # Write current screen name to tomorrow's file
    with open(tomorrow_file, "a") as f:
        f.write(f"\n{next_user_id.strip()}")

    return int(float(next_user_id.strip()))


@solid(config_schema={"timestamp": str})
def get_friends_of_user(context, next_user_id: int) -> List[int]:
    """
    Collects friends of a user and appends it to a csv file.

    Returns the list of integers for friends' ids.
    """
    next_user_id = int(float(next_user_id))

    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )
    friends_ids = get_friends_ids(context.log, next_user_id)

    header = not Path(DAILY_FRIENDS_CHECK_PATH.format(timestamp)).exists()

    pd.DataFrame({"user_id": next_user_id, "friends_id": friends_ids}).to_csv(
        DAILY_FRIENDS_CHECK_PATH.format(timestamp), mode="a", header=header, index=False
    )
    return friends_ids


@solid
def lookup_users_daily(context, users: List[int]):
    """
    Reads in all users from user_tracking, adds new users.

    Uses lock_file_a to lock access to the csv.
    """
    while lock_file_b.exists():
        time.sleep(0.1)

    with open(lock_file_a, "w") as f:
        f.write("")

    try:

        df = pd.read_csv(USER_TRACKER_PATH)
        lookup = set(df["user_id"].unique())

        new_user_ids = [int(float(u)) for u in users if u not in lookup]
        if not new_user_ids:
            return

        new_users = _convert_users_to_records(lookup_users(context.log, new_user_ids))
        pd.concat([df, pd.DataFrame(new_users)]).to_csv(USER_TRACKER_PATH, index=False)
    finally:
        if lock_file_a.exists():
            lock_file_a.unlink()


@solid(config_schema={"timestamp": str})
def collect_tweets_of_users(context, all_tweets: bool = False):
    """
    Collects tweets the user tweets
    """
    initial_timestamp = datetime.now()
    for _ in range(70):
        collect_tweets_of_user(context, all_tweets=all_tweets)
        if datetime.now() - initial_timestamp > timedelta(minutes=1):
            context.log.info("Have been running for over 2 minutes, returning")
            return


def collect_tweets_of_user(context, all_tweets: bool = False):
    """
    Collects tweets the user tweets
    """
    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )

    df = pd.read_csv(USER_TRACKER_PATH).set_index("user_id")
    df_new = df[df["tweets_last_retrieved"].isna()]
    if len(df_new):
        user_id = int(float(df_new.iloc[0].name))
        latest_tweet_id = None
    else:
        user_id = int(float(df.sort_values("tweets_last_retrieved").iloc[0].name))
        latest_tweet_id = int(float(df.sort_values("tweets_last_retrieved").iloc[0]["latest_tweet_id"]))

    if all_tweets:
        statuses = _convert_tweets_to_dataframe(user_id, get_all_most_recent_tweets(context.log, user_id))
        tweet_file_path = Path(TWEET_HISTORY)

    else:
        statuses = _convert_tweets_to_dataframe(user_id, get_user_tweets(context.log, user_id, since_id=latest_tweet_id))
        tweet_file_path = Path(DAILY_TWEETS_PATH.format(timestamp))

    header = not tweet_file_path.exists()
    statuses.to_csv(tweet_file_path, mode="a", header=header, index=False)

    context.log.info(f"Collected {len(statuses)} tweets for user {user_id}")
    while lock_file_a.exists():
        time.sleep(0.01)

    with open(lock_file_b, "w") as f:
        f.write("")

    try:
        df = pd.read_csv(USER_TRACKER_PATH).set_index("user_id")
        df.at[user_id, "tweets_last_retrieved"] = datetime.now()
        if len(statuses):
            df.at[user_id, "latest_tweet_id"] = int(float(statuses.iloc[0]["id"]))
        df.to_csv(USER_TRACKER_PATH)
    finally:
        if lock_file_b.exists():
            lock_file_b.unlink()

    context.log.info(f"Updated user_id {user_id}, {len(statuses)} new tweets")


def _convert_users_to_records(users: List[User]):
    return [
        {
            "user_id": int(float(user.id)),
            # "screen_name": user.screen_name,
            # "name": user.name,
            # "bio": user.description,
            "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        }
        for user in users
    ]


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
        [{"user_id": int(float(user_id)), "id": int(float(tweet.id)), "text": tweet.text, "created_at": tweet.created_at} for tweet in tweets]
    )

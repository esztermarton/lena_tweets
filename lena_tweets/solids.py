import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
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
)
from lena_tweets.scrape_twitter import (
    get_friends,
    get_user_tweets,
    get_friends_ids,
    lookup_users,
)


lock_file_a = Path(__file__).parent / ".lockfile_a"
lock_file_b = Path(__file__).parent / ".lockfile_b"


@solid(config_schema={"timestamp": str})
def collect_user_information(context, individuals_to_monitor: List[str]):
    """
    Collects information about users
    """

    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )

    context.log.info(f"{individuals_to_monitor}")

    df = pd.concat(
        [
            _convert_friends_to_dataframe(*get_friends(screen_name))
            for screen_name in individuals_to_monitor
        ]
    )

    df.to_csv(f"daily_user_scrape_{timestamp}.csv", index=False)
    context.log.info(f"These are the columns of dataframe with type {type(df)}")
    context.log.info(f"{df.to_dict(orient='records')[0].keys()}")
    return list(df["friend_screen_name"].unique())


@solid
def get_ids_collect_info(context):
    """
    Converts a file of screen names to user ids & collects study start info
    """

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)

    with open(STUDY_INPUT_START_PART) as f:
        screen_names = [f.strip() for f in f.readlines() if f.strip()]

    all_users = []
    participants = []
    for screen_name in screen_names:
        user, friends = get_friends(screen_name)
        participants.append(str(user.id))
        all_users.append(user)
        all_users.extend(friends)

    df = _convert_friends_to_dataframe(all_users)
    df.to_csv(STUDY_START_PATH.format(timestamp), index=False)
    tracking_df = df[["user_id"]]
    tracking_df["latest_tweet_id"] = None
    tracking_df["tweets_last_retrieved"] = None
    tracking_df.to_csv(USER_TRACKER_PATH, index=False)

    first_today_file = PARTICIPANTS_QUEUE.format(timestamp)

    context.log.debug(str(participants))

    with open(first_today_file, "w") as f:
        f.writelines(participants)


@solid(config_schema={"timestamp": str})
def collect_tweets_they_see(context, tweets_to_gather: List[str]):
    """
    Collects tweets that each user sees
    """
    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )

    context.log.info(f"{tweets_to_gather}")
    pd.concat(
        [
            _convert_tweets_to_dataframe(screen_name, get_user_tweets(screen_name))
            for screen_name in tweets_to_gather
        ]
    ).to_csv(f"daily_tweet_scrape_{timestamp}.csv", index=False)

    context.log.info("Found some codes")


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

    # # Write rest back to file
    # with open(today_file, "w") as f:
    #     f.writelines(rest)

    # Write current screen name to tomorrow's file
    with open(tomorrow_file, "a") as f:
        f.write(f"\n{next_user_id}")

    return int(next_user_id.strip())


@solid(config_schema={"timestamp": str})
def get_friends_of_user(context, next_user_id: int) -> List[int]:
    """
    Collects friends of a user and appends it to a csv file.

    Returns the list of integers for friends' ids.
    """

    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )
    friends_ids = get_friends_ids(next_user_id)

    pd.DataFrame({"user_id": next_user_id, "friends_id": friends_ids}).to_csv(
        DAILY_FRIENDS_CHECK_PATH.format(timestamp), mode="a", header=False, index=False
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

        new_user_ids = [u for u in users if u not in lookup]
        if not new_user_ids:
            return

        new_users = _convert_users_to_records(lookup_users(new_user_ids))
        pd.concat([df, pd.DataFrame(new_users)]).to_csv(USER_TRACKER_PATH, index=False)
    finally:
        if lock_file_a.exists():
            lock_file_a.unlink()


@solid(config_schema={"timestamp": str})
def collect_tweets_of_user(context):
    """
    Collects tweets the user tweets
    """
    timestamp = context.solid_config.get(
        "timestamp", datetime.now().strftime(TIMESTAMP_FORMAT)
    )

    df = pd.read_csv(USER_TRACKER_PATH).set_index("user_id")
    df_new = df[df["tweets_last_retrieved"].isna()]
    if len(df_new):
        user_id = df_new.iloc[0].name
        latest_tweet_id = None
    else:
        user_id = df.sort_values("tweets_last_retrieved").iloc[0].name
        latest_tweet_id = int(df.sort_values("tweets_last_retrieved").iloc[0]["latest_tweet_id"])

    statuses = _convert_tweets_to_dataframe(user_id, get_user_tweets(user_id, since_id=latest_tweet_id))
    tweet_file_path = Path(DAILY_TWEETS_PATH.format(timestamp))
    if not tweet_file_path.exists():
        statuses.to_csv(
        tweet_file_path, index=False
    )
    else:
        statuses.to_csv(
        tweet_file_path, mode="a", header=False, index=False
    )

    while lock_file_a.exists():
        time.sleep(0.1)

    with open(lock_file_b, "w") as f:
        f.write("")

    try:
        df = pd.read_csv(USER_TRACKER_PATH).set_index("user_id")
        df.at[user_id, "tweets_last_retrieved"] = datetime.now()
        if len(statuses):
            df.at[user_id, "latest_tweet_id"] = int(statuses.iloc[0]["id"])
        df.to_csv(USER_TRACKER_PATH)
    finally:
        if lock_file_b.exists():
            lock_file_b.unlink()

    context.log.info(f"Updated user_id {user_id}, {len(statuses)} new tweets")


def _convert_users_to_records(users: List[User]):
    return [
        {
            "user_id": user.id,
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
        [{"user_id": user_id, "id": tweet.id, "text": tweet.text} for tweet in tweets]
    )

import time
from datetime import datetime
from functools import partial
from typing import List, Optional, Union, Tuple

import tweepy
from tweepy import User, Status

from lena_tweets.auth import authenticate


def retry_decorator(total_retry_number=8):
    def fix_retry_decorator(twitter_func):
        def wrapper(*args, try_number=0, **kwargs):
            try:
                return twitter_func(*args, **kwargs)
            except tweepy.RateLimitError as exc:
                args[0].error(
                    "tweepy.RateLimitError, raising, since this gets handled above."
                )
                raise
            except tweepy.error.TweepError as exc:
                if "Not authorized" in str(exc) or "Sorry, that page does not exist." in str(exc):
                    # No point retrying these
                    raise
                try_number += 1
                if try_number < total_retry_number:
                    args[0].warning(
                        f"TweepError {exc}.\nWill retry {total_retry_number - try_number} more times."
                    )
                    return wrapper(*args, try_number=try_number, **kwargs)
                else:
                    args[0].warning("TweepError. No more retries left")
                    raise

        return wrapper

    return fix_retry_decorator


@retry_decorator()
def _get_data_points(log, func, count: int = 200, cursor: int = -1):
    """
    Gets paginated endpoint where the return has format
    new_data, (cursor x, cursor y)

    Returns list of datapoints
    """
    datapoints = []

    while cursor != 0:
        try:
            new_data, (_, cursor) = func(count=count, cursor=cursor)
        except tweepy.RateLimitError as exc:
            if cursor == -1:
                # Never return -1 due to error so that that is a special case for
                # no friends
                cursor = None
            break
        datapoints.extend(new_data)
        log.info(f"Got {len(new_data)} datapoints")

    return datapoints, cursor


def get_friends(
    log, screen_name: str, count: int = 200, cursor: int = -1
) -> Tuple[User, List[User]]:
    """
    Generates list of people that twitter user with particular handle follows.
    """
    api = authenticate()
    log.info(f"In get friends")
    user = api.get_user(screen_name)
    log.info(f"Fetched user {user.id}")

    try:
        friends, cursor = _get_data_points(
            log, user.friends, count=count, cursor=cursor
        )
    except tweepy.error.TweepError as exc:
        if "Not authorized" in str(exc):
            log.warning(str(exc))
            log.warning(f"WARNING - NO PERMISSIONS TO VIEW friends for {screen_name}")
            friends = []
        else:
            raise
    if cursor != 0 and cursor != -1:
        cursor = cursor or -1
        # This means that while loop didn't break - more than 15 requests necessary
        # for friend. So, recursively get more friends starting from where we left off
        log.error("Rate limit reached. Sleeping for the next round 3 minutes")
        minute = datetime.now().minute
        # Wait for next code...
        while datetime.now().minute == minute or datetime.now().minute % 3 != 0:
            time.sleep(5)
        # Extend with more friends - ignore original uuserr...
        friends.extend(get_friends(log, screen_name, count=count, cursor=cursor)[1])
    log.info(f"Got {len(friends)} new friends")
    return user, friends


def lookup_users(
    log, ids: List[Union[int, str]], screen_name: bool = False
) -> List[User]:
    users = []
    log.info(f"In lookup users")
    for i in range(len(ids) // 100 + 1):
        intex_lower, index_upper = i * 100, (i + 1) * 100
        users.extend(
            lookup_100_friends(
                log, ids[intex_lower:index_upper], screen_name=screen_name
            )
        )
        log.info(f"Extended with {index_upper - intex_lower} users")

    return users


@retry_decorator()
def lookup_100_friends(
    log, ids: List[Union[int, str]], screen_name: bool = False
) -> List[User]:
    api = authenticate()
    if screen_name:
        return api.lookup_users(screen_names=ids)
    return api.lookup_users(user_ids=ids)


@retry_decorator()
def get_friends_ids(log, handle: str, count: int = 5000, cursor: int = -1) -> List[int]:
    """
    Generates list of ids people that twitter user wih particular handle follows.
    """
    log.info(f"Getting friends ids for {handle}")

    api = authenticate()
    friends, cursor = _get_data_points(
        log, partial(api.friends_ids, handle), count=count, cursor=cursor
    )

    if cursor != 0 and cursor != -1:
        cursor = cursor or -1
        # This means that while loop didn't break - more than 15 requests necessary
        # for friend. So, recursively get more friends starting from where we left off
        log.error("Rate limit reached. Sleeping for the next round 3 minutes")
        minute = datetime.now().minute
        # Wait for next code...
        while datetime.now().minute == minute or datetime.now().minute % 3 != 0:
            time.sleep(5)
        # Extend with more friends - ignore original uuserr...
        friends.extend(get_friends_ids(log, screen_name, count=count, cursor=cursor)[1])

    log.info(f"{len(friends)} friends")

    return friends


@retry_decorator()
def get_user_tweets(
    log, user_id: int, since_id: Optional[int] = None, count: int = 200, wait=True
) -> List[Status]:
    """
    Returns tweets of a user
    """
    api = authenticate(wait=wait)
    log.info("Getting user tweets")

    try:
        tweets = api.user_timeline(user_id=user_id, since_id=since_id, count=count)
    except tweepy.error.TweepError as exc:
        if "Not authorized" in str(exc):
            log.warning(str(exc))
            log.warning(f"WARNING - NO PERMISSIONS TO VIEW user_timeline for {user_id}")
            tweets = []
        else:
            raise

    return tweets


@retry_decorator()
def _get_tweets(log, user_id: int, latest_tweet_id: int, count=200) -> List[Status]:
    api = authenticate()

    latest_tweets = api.user_timeline(
        user_id=user_id, max_id=latest_tweet_id, count=count
    )
    timeout = 0
    while not latest_tweets:
        log.info("Latest tweets is empty")
        if timeout > 20:
            return []
        timeout += 1
        latest_tweets = api.user_timeline(
            user_id=user_id, max_id=latest_tweet_id, count=count
        )

    return latest_tweets


@retry_decorator(2)
def get_all_most_recent_tweets(log, user_id: int) -> List[Status]:
    """
    Returns all 3200 retreivable tweets of a user.
    """

    tweets = []
    latest_tweet_id = None

    log.info(f"Getting most recent tweets for user {user_id}")

    api = authenticate()
    try:
        latest_tweets = api.user_timeline(
            user_id=user_id, max_id=latest_tweet_id, count=200
        )
        timeout = 0
        while not latest_tweets:
            log.info("Latest tweets is empty")
            if timeout > 2:
                return []
            timeout += 1
            latest_tweets = api.user_timeline(
                user_id=user_id, max_id=latest_tweet_id, count=200
            )

    except tweepy.error.TweepError as exc:
        if "Not authorized" in str(exc):
            log.warning(str(exc))
            log.warning(f"WARNING - NO PERMISSIONS TO VIEW user_timeline for {user_id}")
            return []
        else:
            raise

    tweets.extend(latest_tweets)

    while latest_tweets and latest_tweets[-1].id != latest_tweet_id:
        log.debug("More tweets to fetch")
        latest_tweet_id = latest_tweets[-1].id
        latest_tweets = _get_tweets(log, user_id, latest_tweets[-1].id, count=200)

        tweets.extend(latest_tweets[1:])

    return tweets

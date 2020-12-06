from functools import partial
from typing import List, Optional, Union, Tuple

import tweepy
from tweepy import User, Status

api_key = ""
key_secret = ""
consumer_key = api_key
consumer_secret = key_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


def _get_data_points(func, count: int = 200):
    """
    Gets paginated endpoint where the return has format
    new_data, (cursor x, cursor y)

    Returns list of datapoints
    """
    datapoints = []
    cursor = -1

    while cursor != 0:
        new_data, (_, cursor) = func(count=count, cursor=cursor)
        datapoints.extend(new_data)

    return datapoints


def get_friends(screen_name: str, count: int = 200) -> Tuple[User, List[User]]:
    """
    Generates list of people that twitter user with particular handle follows.
    """
    user = api.get_user(screen_name)

    friends = _get_data_points(user.friends, count=count)

    return user, friends


def lookup_users(ids: List[Union[int, str]], screen_name: bool = False) -> List[User]:
    users = []
    for i in range(len(ids) // 100 + 1):
        intex_lower, index_upper = i * 100, (i + 1) * 100
        users.extend(
            lookup_100_friends(ids[intex_lower:index_upper], screen_name=screen_name)
        )

    return users


def lookup_100_friends(
    ids: List[Union[int, str]], screen_name: bool = False
) -> List[User]:
    if screen_name:
        return api.lookup_users(screen_names=ids)
    return api.lookup_users(user_ids=ids)


def get_friends_ids(handle: str, count: int = 5000) -> List[int]:
    """
    Generates list of ids people that twitter user wih particular handle follows.
    """

    friends = _get_data_points(partial(api.friends_ids, handle), count=count)

    return friends


def _get_data_points_since(func, since: Optional[int] = None, count: int = 200):
    datapoints = []

    r = func(count=count)
    print(r)
    new_data = r
    datapoints.extend(new_data)

    return datapoints


def get_user_tweets(id: int, since_id: Optional[int] = None) -> List[Status]:
    """
    Returns tweets of a user
    """
    tweets = _get_data_points_since(
        partial(api.user_timeline, user_id=id, since_id=since_id)
    )
    return tweets

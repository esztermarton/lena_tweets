from functools import partial
from typing import List, Optional, Union, Tuple

import tweepy
from tweepy import User, Status

from lena_tweets.auth import authenticate


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
    api = authenticate()
    user = api.get_user(screen_name)

    try:
        friends = _get_data_points(user.friends, count=count)
    except tweepy.error.TweepError as exc:
        if "Not authorized" in str(exc):
            print(exc)
            print(f"WARNING - NO PERMISSIONS TO VIEW friends for {screen_name}")
            friends = []
        else:
            raise

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
    api = authenticate()
    if screen_name:
        return api.lookup_users(screen_names=ids)
    return api.lookup_users(user_ids=ids)


def get_friends_ids(handle: str, count: int = 5000) -> List[int]:
    """
    Generates list of ids people that twitter user wih particular handle follows.
    """

    api = authenticate()
    friends = _get_data_points(partial(api.friends_ids, handle), count=count)

    return friends


def get_user_tweets(user_id: int, since_id: Optional[int] = None, count: int = 200, wait=True) -> List[Status]:
    """
    Returns tweets of a user
    """
    api = authenticate(wait=wait)
    try:
        tweets = api.user_timeline(user_id=user_id, since_id=since_id, count=count)
    except tweepy.error.TweepError as exc:
        if "Not authorized" in str(exc):
            print(exc)
            print(f"WARNING - NO PERMISSIONS TO VIEW user_timeline for {user_id}")
            tweets = []
        else:
            raise
    
    return tweets


def get_all_most_recent_tweets(user_id: int) -> List[Status]:
    """
    Returns all 3200 retreivable tweets of a user.
    """
    api = authenticate()

    tweets = []
    latest_tweet_id = None
    
    latest_tweets = api.user_timeline(user_id=user_id, max_id=latest_tweet_id, count=200)
    tweets.extend(latest_tweets)
    
    counter = 1

    while latest_tweets[-1].id != latest_tweet_id:
        latest_tweet_id = latest_tweets[-1].id

        latest_tweets = api.user_timeline(user_id=user_id, max_id=latest_tweet_id, count=200)
        timeout = 0
        while not latest_tweets:
            if timeout > 10:
                raise RuntimeError
            timeout += 1
            latest_tweets = api.user_timeline(user_id=user_id, max_id=latest_tweet_id, count=200)
            
        counter += 1

        tweets.extend(latest_tweets[1:])

    return tweets

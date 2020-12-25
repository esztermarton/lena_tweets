from datetime import datetime
from typing import Optional

import tweepy

from lena_tweets.config import CREDS


def authenticate(cred_id: Optional[int] = None, wait=False):
    """
    Authenticates with one of a number of credentials configured in config
    
    If credentials contain consumer secret, will use that
    """
    if not CREDS:
        raise ValueError(
            "Fill in with at least 1 set of twitter application credentials to use module"
        )
    minute_of_day = datetime.now().minute + datetime.now().hour * 60
    if cred_id is None:
        cred_id = (minute_of_day // 3) % len(CREDS)

    creds = CREDS[cred_id]

    api_key = creds["API_KEY"]
    key_secret = creds["KEY_SECRET"]
    access_token = creds.get("ACCESS_TOKEN")
    access_token_secret = creds.get("ACCESS_TOKEN_SECRET")
    if not (bool(access_token) is bool(access_token_secret)):
        raise ValueError(
            "Either both or neither one of ACCESS_TOKEN and ACCESS_TOKEN_SECRET must be given"
        )

    auth = tweepy.OAuthHandler(api_key, key_secret)
    if access_token:
        auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=wait, wait_on_rate_limit_notify=wait)
    return api

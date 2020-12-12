import tweepy

from lena_tweets.config import CREDS

def authenticate(cred_id: int = 0, wait=True):
    """
    Authenticates with one of a number of credentials configured in config
    
    If credentials contain consumer secret, will use that
    """
    if not CREDS:
        raise ValueError("Fill in with at least 1 set of twitter application credentials to use module")
    creds = CREDS[cred_id]

    api_key = creds["API_KEY"]
    key_secret = creds["KEY_SECRET"]
    if not (("ACCESS_TOKEN" in creds) == ("ACCESS_TOKEN_SECRET" in creds)):
        raise ValueError("Either both or neither one of ACCESS_TOKEN and ACCESS_TOKEN_SECRET must be given")
    consumer_key = creds.get("ACCESS_TOKEN", api_key)
    consumer_secret = creds.get("ACCESS_TOKEN_SECRET", key_secret)

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=wait, wait_on_rate_limit_notify=wait)
    return api


from contextlib import ContextDecorator
from datetime import datetime

from peewee import (
    JOIN,
    BooleanField,
    CharField,
    ColumnFactory,
    DateTimeField,
    DeferredForeignKey,
    ForeignKeyField,
    BigIntegerField,
    ModelSelect,
    ProgrammingError,
    TextField,
    Model,
)
from playhouse.postgres_ext import BinaryJSONField, PostgresqlExtDatabase
from playhouse.signals import Model

import lena_tweets.config

database = PostgresqlExtDatabase(None, autorollback=True)


class ConnectionContext(ContextDecorator):
    db = None
    tables_created = None
    global_in_context = False

    def __init__(self):
        self.dont_close_connection = False

    def __enter__(self):
        if ConnectionContext.global_in_context:
            self.dont_close_connection = True

        if not ConnectionContext.db:
            ConnectionContext.db = get_database()
        if self.db.is_closed():
            self.db.connect()
            ConnectionContext.global_in_context = True

        try:
            # See if the tables are set up
            Tracker.get_or_none(id=0)
        except ProgrammingError:
            create_tables(self.db)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.db.in_transaction() and not self.dont_close_connection:
            self.db.close()
            ConnectionContext.global_in_context = False


def get_database():
    db_name = lena_tweets.config.DATABASE_NAME
    database.init(
        db_name,
        host=lena_tweets.config.POSTGRES_HOST,
        port=lena_tweets.config.POSTGRES_PORT,
        user=lena_tweets.config.POSTGRES_USER,
        password=lena_tweets.config.POSTGRES_PASSWORD,
    )
    return database


def get_usable_models():
    """
        This collects subclasses of BaseModel to instantiate
        the tables corresponding to the models dynamically.
        Only models derived from EIP's BaseModel will be
        collected.
    """
    models = [Tracker]
    return models


def create_tables(db):
    models = get_usable_models()
    db.create_tables(models)


def drop_tables(db):
    models = get_usable_models()
    db.drop_tables(models, cascade=True)


def connection_manager():
    return ConnectionContext()


class Tracker(Model):
    """
    Tracking object to know who's tweets and users have last been updated.
    """

    user_id = BigIntegerField(unique=True)
    latest_tweet_id = BigIntegerField(null=True)
    tweets_last_retrieved = DateTimeField(null=True)
    friends_last_retrieved = DateTimeField(null=True)
    creation_date = DateTimeField(default=datetime.utcnow())
    participant = BooleanField(default=False)

    class Meta:
        database = database

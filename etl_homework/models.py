import os

from peewee import CharField, TimestampField, FloatField, Model
from playhouse.postgres_ext import JSONField

from etl_homework import dbs


class TimeSeries(Model):
    timeframe = CharField(max_length=32, default="5m")
    start_timestamp = TimestampField(index=True)


class NetworkDifficulty(TimeSeries):
    network = CharField(default="BTC")
    difficulty = FloatField()

    class Meta:
        database = dbs.crawler_db


class NetworkHasRate(TimeSeries):
    network = CharField(default="BTC")

    class Meta:
        database = dbs.crawler_db


class CoinPrice(TimeSeries):
    coin = CharField(default="BTC", max_length=32)
    price = FloatField()

    class Meta:
        database = dbs.crawler_db


class GeneratedData(TimeSeries):
    category = CharField(default="BTCNetworkAnalysisExample", max_length=32)
    data = JSONField()

    class Meta:
        database = dbs.generated_db

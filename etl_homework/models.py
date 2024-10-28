from peewee import CharField, TimestampField, FloatField, Model, BigIntegerField
from playhouse.postgres_ext import JSONField

from etl_homework import dbs


class TimeSeries(Model):
    UNIQUE_INDEX_FILES = ("timeframe", "start_timestamp")

    timeframe = CharField(max_length=32, default="5m")
    start_timestamp = BigIntegerField()  # second as resolution


class NetworkDifficulty(TimeSeries):
    network = CharField(default="BTC")
    difficulty = FloatField()

    class Meta:
        database = dbs.crawler_db_proxy
        indexes = ((("network",) + TimeSeries.UNIQUE_INDEX_FILES, True),)


class NetworkHashRate(TimeSeries):
    network = CharField(default="BTC")
    hash_rate = FloatField()

    class Meta:
        database = dbs.crawler_db_proxy
        indexes = ((("network",) + TimeSeries.UNIQUE_INDEX_FILES, True),)


class CoinPrice(TimeSeries):
    coin = CharField(default="BTC", max_length=32)
    currency = CharField(default="USD", max_length=32)
    price = FloatField()

    class Meta:
        database = dbs.crawler_db_proxy
        indexes = ((("coin", "currency") + TimeSeries.UNIQUE_INDEX_FILES, True),)


class BTCUSDPriceWithNetStat(TimeSeries):
    price = FloatField()
    hash_rate = FloatField()
    difficulty = FloatField()

    class Meta:
        database = dbs.generated_db_proxy
        indexes = ((TimeSeries.UNIQUE_INDEX_FILES, True),)

    def as_list(self, fields):
        values = []
        for field in fields:
            values.append(getattr(self, field))
        return values

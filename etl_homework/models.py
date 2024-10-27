from peewee import CharField, TimestampField, FloatField, Model
from playhouse.postgres_ext import JSONField

from etl_homework import dbs


class TimeSeries(Model):
    timeframe = CharField(max_length=32, default="5m")
    start_timestamp = TimestampField(index=True, resolution=1)  # second as resolution


class TimeSeriesMetaClass:
    indexes = (
        # create a unique on from/to/date
        (("timeframe", "start_timestamp"), True),
    )


class NetworkDifficulty(TimeSeries):
    network = CharField(default="BTC")
    difficulty = FloatField()

    class Meta(TimeSeriesMetaClass):
        database = dbs.crawler_db_proxy


class NetworkHashRate(TimeSeries):
    network = CharField(default="BTC")

    class Meta(TimeSeriesMetaClass):
        database = dbs.crawler_db_proxy


class CoinPrice(TimeSeries):
    coin = CharField(default="BTC", max_length=32)
    currency = CharField(default="USD", max_length=32)
    price = FloatField()

    class Meta(TimeSeriesMetaClass):
        database = dbs.crawler_db_proxy


class GeneratedData(TimeSeries):
    category = CharField(default="BTCNetworkAnalysisExample", max_length=32)
    data = JSONField()

    class Meta(TimeSeriesMetaClass):
        database = dbs.generated_db_proxy

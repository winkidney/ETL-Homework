import logging
import os
import time
from urllib.parse import urljoin

import dotenv
import requests
import ccxt

from etl_homework.constants import TimeFrames
from etl_homework.transformers import generate_tf_indexes, interpolate_data
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


class RequestFailed(ValueError):
    pass


class MemPoolPeriod:
    ONE_MONTH = "1m"
    THREE_DAYS = "1m"

    _str2minutes = {
        "1m": 60 * 24 * 30,
        "3d": 60 * 24 * 3,
    }

    @classmethod
    def is_valid(cls, minutes: str) -> bool:
        return minutes in cls._str2minutes

    def to_int(cls, minutes: str) -> int:
        return cls._str2minutes[minutes]


def is_price_ts_valid(req_ts: int, last_price_ts, num_minutes):
    last_recent_ts = get_last_recent_n_minutes_timestamp(req_ts, num_minutes)
    return last_price_ts == last_recent_ts


class BinanceAPICrawler:
    NUM_MAX_KLINE_CANDLES = 1500

    _s = requests.session()
    _base_url = "https://api.binance.com"

    def __init__(self, logger: logging.Logger = None):
        self._logger = logger or logging.getLogger(__name__)
        # load dotenv in working directory
        dotenv.load_dotenv()
        config = {}
        if "ETL_HTTP_PROXY" in os.environ:
            config["proxies"] = {
                "http": os.environ["ETL_HTTP_PROXY"],
                "https": os.environ["ETL_HTTPS_PROXY"],
            }
        self._ex = ccxt.binance(config)

    def _get_url(self, path: str) -> str:
        return urljoin(self._base_url, path)

    def _get_kline(self, coin: str, currency: str, timeframe: str, limit=None):
        """
        :param coin:
        :param currency:
        :param limit: for binance, default is 500, max is 1500 (about 1500 hours)
            ref: https://developers.binance.com/docs/derivatives/option/market-data/Kline-Candlestick-Data
        :return: float[][bar_start_ts_in_seconds, open, high ,low, close]
        """
        limit = limit if limit else 10
        assert 0 < limit <= 1500
        pair = f"{coin}/{currency}".upper()
        ohlcv = self._ex.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)

        prices = []
        for candle in ohlcv:
            start_timestamp = candle[0] / 1000  # to seconds
            open_, high, low, close = candle[1], candle[2], candle[3], candle[4]
            prices.append((start_timestamp, open_, high, low, close))
        return prices

    def get_historic_price(
        self,
        coin: str = "BTC",
        currency: str = "USD",
        timeframe=TimeFrames.FIVE_MINUTES,
        limit: int = None,
    ):
        """
        :return: list like ((timestamp, price_at_timestamp), )
        """
        req_ts = int(time.time())
        prices = self._get_kline(coin, currency, timeframe=timeframe, limit=limit)
        out_prices = []
        for price in prices:
            start_timestamp = int(price[0])
            open_price = price[1]
            out_prices.append((start_timestamp, open_price))
        if not is_price_ts_valid(
            req_ts,
            last_price_ts=out_prices[-1][0],
            num_minutes=TimeFrames.to_int(timeframe),
        ):
            raise RequestFailed("failed to fetch prices, last timestamp invalid")
        return out_prices


class MemPoolAPICrawler:
    _s = requests.Session()
    _base_url = "https://mempool.space"

    def __init__(self, logger: logging.Logger = None):
        self._logger = logger or logging.getLogger(__name__)

    def _get_url(self, path: str) -> str:
        return urljoin(self._base_url, path)

    def _get_hash_rate(self, period: str) -> dict:
        """
        Ref: https://mempool.space/docs/api/rest#get-hashrate
        :param period: 1m, 3m, 6m, 1y, 2y, 3y
        :return: hash rate in given periods
        """
        if not MemPoolPeriod.is_valid(period):
            raise ValueError(f"invalid period: {period}")

        path = f"/api/v1/mining/hashrate/{period}"
        url = self._get_url(path)

        resp = self._s.get(url)
        if resp.status_code != 200:
            error_msg = f"failed to fetch: url='{url}'"
            self._logger.error(error_msg)
            raise RequestFailed(error_msg)
        return resp.json()

    def _get_historic_difficulty(self, period: str = MemPoolPeriod.THREE_DAYS) -> list:
        """
        Ref: https://mempool.space/docs/api/rest#get-difficulty-adjustments
        :param period: 1m, 3m, 6m, 1y, 2y, 3y
        :return: difficulty in given periods, the data is just like:
            [
              [
                1729620301,
                866880,
                95672703408223.94,
                1.03936
              ],
              [
                1728456399,
                864864,
                92049594548485.48,
                1.04123
              ]
            ]
            with elements [Block timestamp, Block height, Difficulty, Difficulty change]
        """
        if not MemPoolPeriod.is_valid(period):
            raise ValueError(f"invalid period: {period}")

        path = f"/api/v1/mining/difficulty-adjustments/{period}"
        url = self._get_url(path)

        resp = self._s.get(url)
        if resp.status_code != 200:
            error_msg = f"failed to fetch: url='{url}'"
            self._logger.error(error_msg)
            raise RequestFailed(error_msg)
        return resp.json()

    @classmethod
    def normalize_hash_rate_history(
        cls, raw_hash_rates: dict, timeframe=TimeFrames.FIVE_MINUTES
    ) -> tuple:
        """
        :param raw_hash_rates: the data is just like:
        :param timeframe: 1m, 3m, 6m, 1y, 2y, 3y
        {
          "hashrates": [
            {
              "timestamp": 1729900800,
              "avgHashrate": 736575040256689400000
            },
            {
              "timestamp": 1729987200,
              "avgHashrate": 686029213440536600000
            },
            {
              "timestamp": 1730073600,
              "avgHashrate": 766343891160459100000
            }
          ],
          "difficulty": [],
          "currentHashrate": 713196680878333900000,
          "currentDifficulty": 95672703408223.94
        }
        :return: tuple like ((start_timestamp, hash_rate), )
        """
        # TODO(winkidney): use other data source instead of interpolating data ourselves
        minutes = TimeFrames.to_int(timeframe)
        hash_rates = sorted(raw_hash_rates["hashrates"], key=lambda x: x["timestamp"])
        hash_rates.append(
            {
                "timestamp": int(time.time()),
                "avgHashrate": raw_hash_rates["currentHashrate"],
            }
        )
        valid_ts = [
            get_last_recent_n_minutes_timestamp(data["timestamp"], minutes)
            for data in hash_rates
        ]
        valid_data = [data["avgHashrate"] for data in hash_rates]
        larger_range_ts = valid_ts[-1] + 1  # which includes the last timestamp within
        start_ts = valid_ts[0]
        index2interpolate = generate_tf_indexes(
            start_ts, larger_range_ts, time_frame=timeframe
        )
        interpolated = interpolate_data(
            index2interpolate, valid_ts, valid_data, force_int=True
        )
        return tuple(zip(index2interpolate, interpolated))

    @classmethod
    def normalize_difficulty_history(
        cls, raw_difficulty: list, timeframe=TimeFrames.FIVE_MINUTES
    ) -> tuple:
        """
        :param raw_difficulty:
        :param current_difficulty:
        :param timeframe: 1m
        :return: tuple like ((timestamp, value), )
        """
        # TODO(winkidney): use other data source instead of interpolating data ourselves
        minutes = TimeFrames.to_int(timeframe)
        difficulties = [(dft[0], dft[2]) for dft in raw_difficulty]
        difficulties = sorted(difficulties, key=lambda x: x[0])
        # current
        difficulties.append((int(time.time()), difficulties[-1][1]))
        valid_ts = [
            get_last_recent_n_minutes_timestamp(data[0], minutes)
            for data in difficulties
        ]
        valid_data = [data[1] for data in difficulties]
        larger_range_ts = valid_ts[-1] + 1  # which includes the last timestamp within
        start_ts = valid_ts[0]
        index2interpolate = generate_tf_indexes(
            start_ts, larger_range_ts, time_frame=timeframe
        )
        interpolated = interpolate_data(
            index2interpolate, valid_ts, valid_data, force_int=True
        )
        return tuple(zip(index2interpolate, interpolated))

    def get_historic_hash_rate(
        self, period: str = MemPoolPeriod.ONE_MONTH, timeframe=TimeFrames.FIVE_MINUTES
    ):
        raw_hash_rate_data = self._get_hash_rate(period)
        return self.normalize_hash_rate_history(raw_hash_rate_data, timeframe=timeframe)

    def get_historic_difficulty(
        self, period: str = MemPoolPeriod.THREE_DAYS, timeframe=TimeFrames.FIVE_MINUTES
    ):
        raw_difficulty_data = self._get_historic_difficulty(period)
        return self.normalize_difficulty_history(
            raw_difficulty_data, timeframe=timeframe
        )

    def get_current_hash_rate_difficulty(self):
        raw_difficulty_data = self._get_hash_rate(MemPoolPeriod.THREE_DAYS)
        hash_rate = raw_difficulty_data["currentHashrate"]
        difficulty = raw_difficulty_data["currentDifficulty"]
        return hash_rate, difficulty

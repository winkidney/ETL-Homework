import logging
import os
import time
from urllib.parse import urljoin

import requests
import ccxt

from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


class RequestFailed(ValueError):
    pass


class TimeFrames:
    FIVE_MINUTES = "5m"

    _str2minutes = {
        FIVE_MINUTES: 5,
    }

    @classmethod
    def to_int(cls, minutes: str) -> int:
        return cls._str2minutes[minutes]


class HashRatePeriod:
    ONE_MINUTE = "1m"

    _str2minutes = {
        "1m": 1,
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
    _s = requests.session()
    _base_url = "https://api.binance.com"

    def __init__(self):
        config = {}
        if os.environ["HTTP_PROXY"]:
            config["proxies"] = {
                "http": os.environ["HTTP_PROXY"],
                "https": os.environ["HTTPS_PROXY"],
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
        req_ts = int(time.time())
        prices = self._get_kline(coin, currency, timeframe=timeframe, limit=limit)
        out_prices = []
        for price in prices:
            start_timestamp = price[0]
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

    def _get_hashrate(self, period: str) -> dict:
        """
        Ref: https://mempool.space/docs/api/rest#get-hashrate
        :param period: 1m, 3m, 6m, 1y, 2y, 3y
        :return: hash rate with periods
        """
        if not HashRatePeriod.is_valid(period):
            raise ValueError(f"invalid period: {period}")

        path = f"/api/v1/mining/hashrate/{period}"
        url = self._get_url(path)

        resp = self._s.get(url)
        if resp.status_code != 200:
            error_msg = f"failed to fetch: url='{url}'"
            self._logger.error(error_msg)
            raise RequestFailed(error_msg)
        return resp.json()

    def normalize_difficulty(self, difficulty: list) -> list:
        # TODO(winkidney): implementation
        return difficulty

    def get_historic_hash_rate(self, period: str = HashRatePeriod.ONE_MINUTE) -> float:
        difficulity = self._get_hashrate(period)
        self.normalize_difficulty(difficulity)
        return difficulity
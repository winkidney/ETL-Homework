from etl_homework import models


class CoinMetrics:

    @classmethod
    def merge_coin_metric(
        cls,
        price: models.CoinPrice,
        nd: models.NetworkDifficulty,
        nhr: models.NetworkHashRate,
    ) -> (models.BTCUSDPriceWithNetStat, bool):
        if not (
            price.start_timestamp == nd.start_timestamp
            and nd.start_timestamp == nhr.start_timestamp
        ):
            raise ValueError(
                "failed to verify coin metrics, start_timestamp not the same"
            )
        obj, created = models.BTCUSDPriceWithNetStat.get_or_create(
            timeframe=price.timeframe,
            start_timestamp=price.start_timestamp,
            defaults=dict(
                price=price.price,
                hash_rate=nhr.hash_rate,
                difficulty=nd.difficulty,
            ),
        )
        return obj, created

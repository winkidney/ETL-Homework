import peewee
import pytest

from etl_homework import models


def test_should_write_repeated_data_raise_error(database):
    with pytest.raises(peewee.IntegrityError) as e:
        data_args = dict(
            timeframe="5m",
            start_timestamp=1000000,
            difficulty=10,
        )
        new_model = models.NetworkDifficulty(**data_args)
        new_model.save()
        new_model = models.NetworkDifficulty(**data_args)
        new_model.save()
    assert (
        e.value.args[0]
        == "UNIQUE constraint failed: networkdifficulty.timeframe, networkdifficulty.start_timestamp"
    )

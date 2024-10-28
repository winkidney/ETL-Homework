import peewee
import pytest

from etl_homework import models


@pytest.mark.parametrize(
    "difficulty1, difficulty2",
    (
        (10, 10),
        (10, 20),
    ),
)
def test_should_write_repeated_data_raise_error(difficulty1, difficulty2, database):
    defaults = dict(
        difficulty=difficulty1,
    )
    data_args = dict(timeframe="5m", start_timestamp=1000000, defaults=defaults)
    new_model, created1 = models.NetworkDifficulty.get_or_create(**data_args)
    new_model.save()
    data_args["defaults"]["difficulty"] = difficulty2
    new_model, created2 = models.NetworkDifficulty.get_or_create(**data_args)
    new_model.save()
    assert created1
    assert not created2

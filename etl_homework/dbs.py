import functools
import os

from peewee import SqliteDatabase, DatabaseProxy

PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

crawler_db_proxy = DatabaseProxy()
generated_db_proxy = DatabaseProxy()

_inited = False


def is_initialized():
    global _inited
    return _inited


def bind_production_db():
    global _inited
    if not _inited:
        crawler_db = SqliteDatabase(os.path.join(PROJECT_PATH, "crawler_data.sqlite"))
        generated_db = SqliteDatabase(
            os.path.join(PROJECT_PATH, "generated_data.sqlite")
        )
        crawler_db_proxy.initialize(crawler_db)
        generated_db_proxy.initialize(generated_db)
        _inited = True


def with_db(fn: callable):
    global _inited

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not is_initialized():
            bind_production_db()
        return fn(*args, **kwargs)

    return wrapper


def bind_test_db():
    global _inited

    crawler_db = SqliteDatabase(":memory:")
    generated_db = SqliteDatabase(":memory:")

    crawler_db_proxy.initialize(crawler_db)
    generated_db_proxy.initialize(generated_db)

    _inited = True

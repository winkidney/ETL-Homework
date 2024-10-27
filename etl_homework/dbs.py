import os

from peewee import SqliteDatabase, DatabaseProxy

PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

crawler_db_proxy = DatabaseProxy()
generated_db_proxy = DatabaseProxy()


def bind_production_db():
    crawler_db = SqliteDatabase("crawler_data.sqlite")
    generated_db = SqliteDatabase("generated_data.sqlite")

    crawler_db_proxy.initialize(crawler_db)
    generated_db_proxy.initialize(generated_db)


def bind_test_db():
    crawler_db = SqliteDatabase(":memory:")
    generated_db = SqliteDatabase(":memory:")

    crawler_db_proxy.initialize(crawler_db)
    generated_db_proxy.initialize(generated_db)

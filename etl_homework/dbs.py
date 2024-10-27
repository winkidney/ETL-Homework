import os

from peewee import SqliteDatabase


PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

crawler_db = SqliteDatabase("crawler_data.sqlite")
generated_db = SqliteDatabase("generated_data.sqlite")

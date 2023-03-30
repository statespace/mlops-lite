import sqlalchemy
import os
import sys
import inspect

from datamodel import (
    DataRegistry,
    DataDefinitions
)

from datamodel import get_datamodel_table_names

def default_sqlite_uri() -> str:
    return "sqlite:///sqlite/mlops-lite.db"

class DataBase:

    def __init__(self, db_uri: str | None = default_sqlite_uri()) -> None:
        self.engine = sqlalchemy.create_engine(db_uri)
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)

        # check if DB is up to date / or exists at all
        db_tables = sqlalchemy.inspect(self.engine).get_table_names()
        datamodel_tables = get_datamodel_table_names()
        if not all([i in db_tables for i in datamodel_tables]): # this does not ensure that all columns within tables are as expected!
            print('DB is not complete') # procede with migration
    
    @staticmethod
    def construct_uri_from_env():

        # meant for connecting to Postgres, probably needs a config that defines DB choice

        db_conn = os.environ["DB_CONNECTION"]
        db_user = os.environ["DB_USERNAME"]
        db_pass = os.environ["DB_PASSWORD"]
        db_host = 'localhost' if "TEST" in os.environ else os.environ["DB_HOST"]
        db_port = os.environ["DB_PORT"]
        db_name = os.environ["DB_DATABASE"]

        constring = f'{db_conn}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

        return constring
    


from time import time

import pandas as pd
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Select

from mlopslite.registry.datamodel import Base, get_datamodel_table_names
from mlopslite.registry.registryconfig import RegistryConfig


class DataBase:
    def __init__(self, config: RegistryConfig) -> None:
        self.url = config.db_constring
        self.engine = create_engine(self.url)
        self.session = sessionmaker(bind=self.engine)

        # check if DB is up to date / or exists at all
        db_tables = inspect(self.engine).get_table_names()
        datamodel_tables = get_datamodel_table_names()
        if not all([i in db_tables for i in datamodel_tables]):
            # this does not ensure that all columns within tables are as expected!
            print("DB is not complete")  # procede with migration
            self._upgrade_db()
        else:
            print("Database Ready!")

    def _upgrade_db(self):
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", self.url)

        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            command.revision(config, f"{int(time())}_update", autogenerate=True)
            command.upgrade(config, "heads")

    def execute_select_query(self, statement: Select) -> pd.DataFrame:
        """
        Returns query in pd.DataFrame form
        """

        with self.session.begin() as con:
            result = con.execute(statement)
            keys = result.keys()
            data = result.all()

            dataset = [{k: v for k, v in zip(keys, item)} for item in data]

            df = pd.DataFrame(dataset)

        return df

    def execute_select_query_single(self, statement: Select):
        with self.session.begin() as con:
            result = con.execute(statement)
            data = result.all()

        return data[0][0]

    def execute_insert_query_single(self, model: Base):
        with self.session.begin() as con:
            con.add(model)
            con.commit()

    #### implementing other DBs

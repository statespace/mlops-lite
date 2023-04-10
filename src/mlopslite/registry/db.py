from time import time

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Select

from mlopslite.registry.datamodel import (Base, DataRegistry,
                                          DataRegistryColumns,
                                          get_datamodel_table_names)
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

        #print(config)

        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            command.revision(config, f"{int(time())}_update", autogenerate=True)
            command.upgrade(config, "heads")

    def execute_select_query(self, statement: Select) -> list[dict]:
        """
        Returns query in pd.DataFrame form
        """

        with self.session.begin() as con:
            result = con.execute(statement)
            keys = result.keys()
            data = result.all()

            response = [{k: v for k, v in zip(keys, item)} for item in data]

        return response

    def execute_select_query_single(self, statement: Select):
        with self.session.begin() as con:
            result = con.execute(statement)
            data = result.all()

        return data[0][0]

    def execute_insert_query_single(self, model: Base):
        with self.session.begin() as con:
            con.add(model)
            con.commit()

    def get_dataset_version_increment(self, name: str) -> int:
        stmt = (
            select(func.max(DataRegistry.version).label("version"))
            .where(DataRegistry.name == name)
            .group_by(DataRegistry.name)
        )

        response = self.execute_select_query(stmt)

        if len(response) == 0:
            return 1
        else:
            return response[0]["version"] + 1

    def get_reference_by_hash(self, hash: str) -> dict | None:
        stmt = select(
            DataRegistry.id,
            DataRegistry.name,
            DataRegistry.version,
            DataRegistry.hash,
        ).where(DataRegistry.hash == hash)

        response = self.execute_select_query(stmt)
        return None if len(response) == 0 else response[0]

    def insert_dataset_returning_reference(self, dr: DataRegistry) -> dict:
        with self.session.begin() as session:
            session.add(dr)
            session.commit()

            out = {"id": dr.id, "name": dr.name, "version": dr.version, "hash": dr.hash}

            return out

    def select_dataset_by_id(self, id: int) -> dict:
        stmt_dataset = select(*DataRegistry.__table__.columns).where(
            DataRegistry.id == id
        )
        stmt_columns = select(*DataRegistryColumns.__table__.columns).where(
            DataRegistryColumns.data_registry_id == id
        )

        return {
            "dataset": self.execute_select_query(stmt_dataset)[0],
            "columns": self.execute_select_query(stmt_columns),
        }

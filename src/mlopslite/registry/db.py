from time import time

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, inspect, select, delete, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Select

from mlopslite.registry.datamodel import (Base, 
                                          DatasetRegistry, 
                                          DeployableRegistry,
                                          DatasetRegistryColumns, 
                                          ExecutionLog, 
                                          get_datamodel_table_names)
from mlopslite.registry.registryconfig import RegistryConfig
from mlopslite.alembic_setup import get_alembic_ini, get_migration_script_location


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
            # needs to thoroughly against the datamodel... can this be done via Base against engine?
            # print("DB is not complete")  # procede with migration
            self._upgrade_db()
        else:
            print("Database Ready!")

    def _upgrade_db(self):
        config = Config(get_alembic_ini())
        config.set_main_option("script_location", get_migration_script_location())
        config.set_main_option("sqlalchemy.url", self.url)

        #print(config)

        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            # there should probably be a script that regenerates migrations on version update..?
            #if regenerate:
            #    command.revision(config, f"{int(time())}_update", autogenerate=True) 
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
            select(func.max(DatasetRegistry.version).label("version"))
            .where(DatasetRegistry.name == name)
            .group_by(DatasetRegistry.name)
        )

        response = self.execute_select_query(stmt)

        if len(response) == 0:
            return 1
        else:
            return response[0]["version"] + 1

    def get_dataset_reference_by_hash(self, hash: str) -> dict | None:
        stmt = select(
            DatasetRegistry.id,
            DatasetRegistry.name,
            DatasetRegistry.version,
            DatasetRegistry.hash,
            DatasetRegistry.created_at
        ).where(DatasetRegistry.hash == hash)

        response = self.execute_select_query(stmt)
        return None if len(response) == 0 else response[0]

    def insert_dataset_returning_reference(self, dr: DatasetRegistry) -> dict:
        with self.session.begin() as session:
            session.add(dr)
            session.flush()
            out = {"id": dr.id, "name": dr.name, "version": dr.version, "hash": dr.hash}
            #session.commit()
            #stmt = insert(dr).returning(dr.id, dr.name, dr.version)
            #return self.execute_select_query(stmt)
            return out

    def select_dataset_by_id(self, id: int) -> dict:
        stmt_dataset = select(*DatasetRegistry.__table__.columns).where(
            DatasetRegistry.id == id
        )
        stmt_columns = select(*DatasetRegistryColumns.__table__.columns).where(
            DatasetRegistryColumns.dataset_registry_id == id
        )

        return {
            "dataset": self.execute_select_query(stmt_dataset)[0],
            "columns": self.execute_select_query(stmt_columns),
        }
    
    def list_datasets(self) -> dict:

        stmt = (select(DatasetRegistry.id,
                      DatasetRegistry.name,
                      DatasetRegistry.version,
                      DatasetRegistry.size_cols,
                      DatasetRegistry.size_rows,
                      DatasetRegistry.description,
                      DatasetRegistry.created_at
                      )
                .order_by(DatasetRegistry.id)
        )
        
        return self.execute_select_query(stmt)
    
    def delete_dataset(self, id: int):

        with self.session.begin() as session:
            stmt = delete(DatasetRegistry).where(DatasetRegistry.id == id)
            session.execute(stmt)

    def get_deployable_reference_by_hash(self, hash: str) -> dict:

        stmt = select(
            DeployableRegistry.id,
            DeployableRegistry.name,
            DeployableRegistry.version,
            DeployableRegistry.hash
        ).where(
            DeployableRegistry.hash == hash
        )

        response = self.execute_select_query(stmt)
        return None if len(response) == 0 else response[0]
    
    def insert_deployable_returning_reference(self, mr: DeployableRegistry) -> dict:
        with self.session.begin() as session:
            session.add(mr)
            session.flush()
            out = {"id": mr.id, "name": mr.name, "version": mr.version, "hash": mr.hash}

            return out
        
    def get_deployable_version_increment(self, name: str, dataset_id: int, target: str) -> int:
        stmt = (
            select(func.max(DeployableRegistry.version).label("version"))
            .where(and_(
                DeployableRegistry.name == name, 
                DeployableRegistry.dataset_registry_id == dataset_id, 
                DeployableRegistry.target == target))
            .group_by(DeployableRegistry.name)
        )

        response = self.execute_select_query(stmt)

        if len(response) == 0:
            return 1
        else:
            return response[0]["version"] + 1
        
    def select_deployable_by_id(self, id: int) -> dict:
        stmt = select(*DeployableRegistry.__table__.columns).where(
            DeployableRegistry.id == id
        )

        response = self.execute_select_query(stmt)

        return response[0]
    
    def list_deployables(self):
        
        stmt = select(
            DeployableRegistry.id,
            DeployableRegistry.name,
            DeployableRegistry.version,
            DeployableRegistry.dataset_registry_id,
            DeployableRegistry.estimator_class,
            DeployableRegistry.estimator_type,
            DeployableRegistry.description,
            DeployableRegistry.created_at
        ).order_by(DeployableRegistry.id)

        return self.execute_select_query(stmt)
    
    def log_execution(self, log: ExecutionLog):
        with self.session.begin() as session:
            session.add(log)
            session.flush()
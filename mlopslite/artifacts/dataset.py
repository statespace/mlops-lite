from mlopslite.registry import datamodel
import pandas as pd
from sqlalchemy.sql import select
from mlopslite.registry.db import DataBase
import json


class DataSet:
        
    def __init__(self, dataset: pd.DataFrame | dict, target: str) -> None:
        
        if isinstance(dataset, dict):
            dataset = self.convert_dataset_dict_to_pandas(dataset)
        
        if not isinstance(dataset, pd.DataFrame):
            raise NotImplementedError
        
        self.dataset = dataset
        
        self.columns = list(dataset.columns)

        if target not in self.columns:
            raise ValueError("Target column name not found in the dataset")
        
        self.target = target

        self.row_size = dataset.shape[0]
        self.col_size = dataset.shape[1]

        self.dtypes = dict(dataset.dtypes)

    @staticmethod
    def list(db: DataBase) -> dict:
        """
        List all datasets in registry
        """

        statement = select(
            datamodel.DataRegistry.id,
            datamodel.DataRegistry.name,
            datamodel.DataRegistry.description,
            datamodel.DataRegistry.row_size,
            datamodel.DataRegistry.col_size
        )

        return db.execute_select_query(statement)
        
    @staticmethod
    def load(db: DataBase, id: int) -> None: 
        """
        Check and bind existing dataset to MlopsLite object
        """
        
        statement = select(
            datamodel.DataRegistry.data
        ).where(
            datamodel.DataRegistry.id == id
        )

        return db.execute_select_query_single(statement)

    def register(self, db: DataBase, name: str, description: str = "", version: int = 1) -> None:
        """
        Add new dataset to the registry
        """
        
        dm = datamodel.DataRegistry(
            name = name,
            version = version,
            description = description,
            data = json.dumps(self.dataset.to_dict('list')),
            row_size = self.row_size,
            col_size = self.col_size
        )

        db.execute_insert_query_single(dm)

    def convert_dataset_dict_to_pandas(self, dataset: dict):

        return pd.DataFrame(dataset)

    # transform dataset to and from JSON (most likely data format - pd.DataFrame)
    # count rows / columns
    # get all names/dtypes/purpose of columns -> data definition config
    # create pydantic datamodel from config?



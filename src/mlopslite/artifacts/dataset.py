import json

import numpy as np
import pandas as pd
from pandas.core.series import convert_dtypes
from sqlalchemy.sql import select

from mlopslite.registry import datamodel
from mlopslite.registry.db import DataBase


class DataSet:

    """
    Registers, transforms, stores, versions and pulls data artifacts.
    Registering dataset pushes it to the DataBase. All data fields are transformed to either int or str,
    and stored as JSON. Registration creates metadata of the dataset, allowing it to transform to the
    original form.

    TODO: original forms - dict, polars, ndarray

    """

    def __init__(self) -> None:
        self.data = None

    def register(
        self,
        dataset: pd.DataFrame | dict,
        target: str,
        name: str,
        version: int,
        autoload: bool = False,
    ):
        # check if valid
        if not self._check_valid(dataset=dataset):
            raise ValueError("Invalid data")

        df = self._convert_to_pandas(dataset=dataset)

        # construct metadata
        metadata = self._create_dataset_metadata(df=df, target=target)
        # transform to unified format
        conv_data = self._apply_conversion(df=df, metadata=metadata)
        # calc stats
        column_metadata = {k: self._create_column_metadata(v) for k, v in conv_data}
        conv_json = json.dumps(conv_data)
        # write to DB

        # (opt) load and bind dataset

    def _check_valid(self, dataset: pd.DataFrame | dict) -> bool:
        # TODO: check if various possible input types can be coerced to pd.DataFrame
        # TODO: return checklist instead of bool
        return isinstance(dataset, pd.DataFrame)

    def _convert_to_pandas(self, dataset: pd.DataFrame | dict) -> pd.DataFrame:
        # depending on input data types, transform to pd.DataFrame
        if isinstance(dataset, dict):
            dataset = pd.DataFrame(dataset)
        return dataset

    def _create_dataset_metadata(self, df: pd.DataFrame, target: str):
        """
        - dataset size
        - columns
        - target column name
        - original column dtypes
        - converted column dtypes (+ conversion table)

        ! test conversion dtypes orig -> conv -> orig

        """

        metadata = {
            "size": df.shape,
            "colnames": df.columns,
            "target": target,
            "original_dtypes": {k: v.type for k, v in dict(df.dtypes).items()},
            "converted_dtypes": self._type_conversion(df=df),
        }

        return metadata

    def _type_conversion(self, df: pd.DataFrame) -> dict:
        # caveat: pandas breaks int arrays that have None values
        # it might be fixed with pandas 2.+ arrow backend
        conv_dtypes = {k: v.kind for k, v in dict(df.dtypes).items()}

        mapper = {"i": int, "?": bool, "u": int, "f": float, "M": int, "O": str}
        return {k: mapper[v] for k, v in conv_dtypes.items()}

    def _apply_conversion(self, df: pd.DataFrame, metadata: dict) -> dict:
        nonetypes = [np.nan, None]
        conv_dict = df.to_dict("list")

        # check if match cols
        if not all(
            [i in conv_dict.keys() for i in metadata["converted_dtypes"].keys()]
        ):
            raise Exception("Mismatch in columns!")

        out = {}
        for k, v in conv_dict.items():
            target_type = metadata["converted_dtypes"][k]
            out[k] = [target_type(i) if i not in nonetypes else None for i in v]

        return out

    def _create_column_metadata(self, column: list):
        # summarize col
        summary = {
            "null_count": len([i for i in column if i is None]),
            "unique_count": len(set([i for i in column if i is not None])),
            "min_value_num": min([i for i in column if i is not None]),
            "max_value_num": max([i for i in column if i is not None]),
            "unique_vals": list(set(column)),
        }
        return summary

    @staticmethod
    def list(db: DataBase) -> pd.DataFrame:
        """
        List all datasets in registry
        """

        statement = select(
            datamodel.DataRegistry.id,
            datamodel.DataRegistry.name,
            datamodel.DataRegistry.description,
            datamodel.DataRegistry.row_size,
            datamodel.DataRegistry.col_size,
        )

        return db.execute_select_query(statement)

    def load(self, db: DataBase, id: int) -> None:
        """
        Check and bind existing dataset to MlopsLite object
        """

        statement = select(datamodel.DataRegistry.data).where(
            datamodel.DataRegistry.id == id
        )

        return db.execute_select_query_single(statement)

    def write_to_registry(
        self, db: DataBase, name: str, description: str = "", version: int = 1
    ) -> None:
        """
        Add new dataset to the registry
        """

        dm = datamodel.DataRegistry(
            name=name,
            version=version,
            description=description,
            data=json.dumps(self.dataset.to_dict("list")),
            row_size=self.row_size,
            col_size=self.col_size,
        )

        db.execute_insert_query_single(dm)

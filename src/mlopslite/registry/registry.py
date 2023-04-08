from dataclasses import dataclass

import pandas as pd

from mlopslite.artifacts.dataset import DataSet
from mlopslite.artifacts.dataset_metadata import DataSetMetadata
from mlopslite.registry import datamodel
from mlopslite.registry.db import DataBase
from mlopslite.registry.filesystem import FileSystem
from mlopslite.registry.registryconfig import RegistryConfig


class Registry:
    def __init__(self, config: RegistryConfig):
        pass

    # db: DataBase
    # fs: FileSystem
    # config: RegistryConfig

    def pull_dataset_from_registry(self, id: int) -> DataSet:
        pass

    def push_dataset_to_registry(
        self, dataset: str, metadata: DataSetMetadata
    ) -> int:
        
        """
        Add new dataset to the registry
        """
        
        # create DataSet metadata


        self.db.execute_insert_query_single(dm)

        @staticmethod
    def register(
        registry: Registry,
        dataset: pd.DataFrame | dict,
        target: str,
        name: str,
        description: str = "",
    ):
        # check if valid
        if not self._check_valid(dataset=dataset):
            raise ValueError("Invalid data")

        df = convert_to_pandas(dataset=dataset)

        # construct metadata
        metadata = DataSetMetadata.create(
            df=df, target=target, name=name, description=description
        )
        # transform to unified format
        conv_data = convert_df_to_dict(df=df, metadata=metadata)
        # create hash and check against DB for existing datasets
        hash = md5(conv_data.encode("utf-8")).hexdigest()
        registry_ref = self._check_hash_in_registry(hash)

        if registry_ref is None:
            registry_ref = push_dataset_to_registry()

        pull_dataset_from_registry(registry_ref)

    def _check_valid(self, dataset: pd.DataFrame | dict) -> bool:
        # TODO: check if various possible input types can be coerced to pd.DataFrame
        # TODO: return checklist instead of bool
        return isinstance(dataset, pd.DataFrame)

    def _check_hash_in_registry(self, hash: str) -> int | None:
        pass

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
        Load and attach dataset + metadata to instance
        """

        statement = select(datamodel.DataRegistry.data).where(
            datamodel.DataRegistry.id == id
        )


def convert_to_pandas(dataset: pd.DataFrame | dict) -> pd.DataFrame:
    # depending on input data types, transform to pd.DataFrame
    if isinstance(dataset, dict):
        dataset = pd.DataFrame(dataset)
    return dataset


def convert_df_to_dict(df: pd.DataFrame, metadata: DataSetMetadata) -> str:
    nonetypes = [np.nan, None]
    conv_dict = df.to_dict("list")

    # check if match cols
    if not all([i in conv_dict.keys() for i in metadata.converted_dtypes]):
        raise Exception("Mismatch in columns!")

    out = {}
    for k, v in conv_dict.items():
        target_type = metadata.converted_dtypes[k]
        out[k] = [target_type(i) if i not in nonetypes else None for i in v]

    out = json.dumps(out, sort_keys=True, indent=2)

    return out

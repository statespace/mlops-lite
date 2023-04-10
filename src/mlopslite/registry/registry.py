import json
from hashlib import md5

import pandas as pd

from mlopslite.artifacts.metadata import ColumnMetadata, DataSetMetadata
from mlopslite.artifacts.dataset import DataSet
from mlopslite.registry import datamodel
from mlopslite.registry.db import DataBase
from mlopslite.registry.registryconfig import RegistryConfig


class Registry:
    def __init__(self, config: RegistryConfig = RegistryConfig()):
        self.db = DataBase(config=config)
        self.config = config

    # db: DataBase
    # fs: FileSystem
    # config: RegistryConfig

    def pull_dataset_from_registry(self, id: int) -> DataSet:
        ds = self.db.select_dataset_by_id(id)

        dataset = ds["dataset"]["data"]

        dtype_map = {i["column_name"]: i["original_dtype"] for i in ds["columns"]}
        dataset = pd.DataFrame(dataset).astype(dtype=dtype_map)

        metadata = DataSetMetadata.create(
            data = dataset, 
            name = ds["dataset"]["name"], 
            version = ds["dataset"]["version"], 
            description=ds["dataset"]["description"]
        )

        return DataSet(data=dataset, metadata=metadata)

    def push_dataset_to_registry(self, data: dict, metadata: DataSetMetadata) -> dict:
        """
        Add new dataset to the registry
        """

        # compute hash of data
        hash = self.get_data_hash(data)
        registry_ref = self.db.get_reference_by_hash(hash)
        if registry_ref is not None:
            print("Dataset already exists, returning reference instead of pushing!")
            return registry_ref

        # dataset table

        dr = datamodel.DataRegistry(
            name=metadata.name,
            version=self.db.get_dataset_version_increment(metadata.name),
            description=metadata.description,
            data=data,
            size_rows=metadata.size_rows,
            size_cols=metadata.size_cols,
            hash=hash,
        )

        for i in metadata.column_metadata:
            drc = datamodel.DataRegistryColumns(**i.__dict__, data = dr)
            dr.columns.append(drc)

        registry_ref = self.db.insert_dataset_returning_reference(dr)

        return registry_ref

    def get_data_hash(self, data: dict) -> str:
        return md5(
            json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
        ).hexdigest()

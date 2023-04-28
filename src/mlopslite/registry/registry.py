import pandas as pd

from mlopslite.artifacts.metadata import DataSetMetadata
from mlopslite.artifacts.dataset import DataSet
from mlopslite.artifacts.model import Deployable
from mlopslite.registry import datamodel
from mlopslite.registry.db import DataBase
from mlopslite.registry.registryconfig import RegistryConfig
from datetime import datetime


class Registry:
    def __init__(self, config: RegistryConfig):
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
            id = ds['dataset']['id'],
            description=ds["dataset"]["description"]
        )

        return DataSet(data=dataset, metadata=metadata)

    def push_dataset_to_registry(self, dataset: DataSet) -> dict:
        """
        Add new dataset to the registry
        """

        hash = dataset.get_data_hash()        
        registry_ref = self.db.get_dataset_reference_by_hash(hash)
        if registry_ref is not None:
            print("Dataset already exists, returning referenced dataset instead of pushing!")
            return registry_ref

        # dataset table

        dr = datamodel.DatasetRegistry(
            name=dataset.metadata.name,
            version=self.db.get_dataset_version_increment(dataset.metadata.name),
            description=dataset.metadata.description,
            data=dataset.convert_to_dict(),
            size_rows=dataset.metadata.size_rows,
            size_cols=dataset.metadata.size_cols,
            hash=hash,
        )

        for i in dataset.metadata.column_metadata:
            drc = datamodel.DatasetRegistryColumns(**i.__dict__, data = dr)
            dr.columns.append(drc)

        registry_ref = self.db.insert_dataset_returning_reference(dr)

        return registry_ref
    
    def push_model_to_registry(self, deployable: Deployable) -> dict:

        hash = deployable.get_data_hash()
        registry_ref = self.db.get_model_reference_by_hash(hash)
        if registry_ref is not None:
            print("Model already exists, returning referenced model instead of pushing!")
            return registry_ref
        
        mr = datamodel.DeployableRegistry(
            dataset_registry_id = deployable.metadata.dataset_registry_id,
            name=deployable.metadata.name, 
            version=self.db.get_model_version_increment(
                name = deployable.metadata.name, 
                dataset_id=deployable.metadata.dataset_registry_id, 
                target = deployable.metadata.target
            ),
            target = deployable.metadata.target, 
            target_mapping=deployable.metadata.target_mapping, 
            description=deployable.metadata.description,
            estimator_type=deployable.metadata.estimator_type, 
            estimator_class=deployable.metadata.estimator_class,
            deployable=deployable.serialize_deployable(),
            variables = deployable.metadata.variables,
            hash=deployable.get_data_hash()
        )

        registry_ref = self.db.insert_model_returning_reference(mr=mr)

        return registry_ref
    
    def pull_model_from_registry(self, id: int) -> Deployable:
        registry_item = self.db.select_model_by_id(id)
        return Deployable.restore(registry_item)
    
    def log_execution(self, deployable_id: int, input: dict, output: dict) -> None:

        
        # some additional validation, sanity check, logging etc goes here before sending to DB

        root_entry = datamodel.ModelExecutionLog(
            deployable_id=deployable_id, 
            request_time=datetime.utcnow(),
            request_size=len(input)
        )

        for item in zip(input, output):

            #print(item[1]['reference_id'])
            execution_items_link = datamodel.ExecutionItems(
                    reference_id=item[1]['reference_id'],
                    execution_log_item = root_entry
                )

            root_entry.execution_log.append(execution_items_link)
            

            for k,v in item[0].items():

                if k not in ['reference_id']:

                    request = datamodel.RequestItems(
                        varname=k,
                        in_value=v, 
                        data=execution_items_link
                    )

                    execution_items_link.request_items.append(request)

            for k,v in item[1]['results'].items():

                response = datamodel.ResponseItems(
                    classname=str(k),
                    out_value=v,
                    data = execution_items_link
                )

                execution_items_link.response_items.append(response)

        self.db.log_execution(root_entry)

                



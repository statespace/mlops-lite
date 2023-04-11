import pandas as pd

from mlopslite.artifacts.dataset import DataSet
from mlopslite.registry.registry import Registry
from mlopslite.registry.registryconfig import RegistryConfig


class MlopsLite:

    """
    Main class that implements control over the registry - pushing and pulling artifacts

    Expected training workflow:
        - register / bind dataset
        - pull it from DB
        - SKlearn loop -> Pipeline (API definitions should perfectly match) -> joblib
        - push joblib as bytes to registry + precalc all all the stats in json
        - cross compare Pipelines that are bound to same dataset (again, mlops does all the stats)
    """

    def __init__(self, config: RegistryConfig = RegistryConfig()) -> None:
        # set up Registry object
        self.registry = Registry(config=config)  # default to sqlite, workspace folder sqlite/mlops-lite.db

    def create_dataset(self, data, name: str, description: str = "", push: bool = True):
        dataset = DataSet.create(data = data, name = name, description=description)
        if push:
            self.push_dataset(dataset=dataset)
        else:
            self.dataset = dataset

    def pull_dataset(self, id: int) -> None:
        self.dataset = self.registry.pull_dataset_from_registry(id = id)

    def push_dataset(self, dataset: DataSet) -> None:
        self.dataset = self.registry.push_dataset_to_registry(dataset=dataset)

    def list_datasets(self) -> pd.DataFrame:
        
        dslist = self.registry.db.list_datasets()
        return pd.DataFrame(dslist)




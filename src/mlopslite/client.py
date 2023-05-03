import pandas as pd
from typing import Any

from mlopslite.artifacts.dataset import Dataset, create_dataset
from mlopslite.registry.registry import Registry
from mlopslite.registry.registryconfig import RegistryConfig
from mlopslite.artifacts.deployable import Deployable, create_deployable

class MlopsLite:

    """
    Main class that implements control over the registry:
    - pushing and pulling artifacts
    - listing and inspecting elements
    """

    def __init__(self, config: RegistryConfig = RegistryConfig()) -> None:
        # set up Registry object
        self.registry = Registry(config=config)  # default to sqlite, workspace folder sqlite/mlops-lite.db

    def bind_dataset(self, data, name: str, description: str = ""):
        dataset = create_dataset(data = data, name = name, description=description)
        self.push_dataset(dataset=dataset)

    def pull_dataset(self, id: int) -> None:
        self.dataset = self.registry.pull_dataset_from_registry(id = id)

    def push_dataset(self, dataset: Dataset) -> None:
        # pushing also pulls back the dataset, to populate proper references
        registry_ref = self.registry.push_dataset_to_registry(dataset=dataset)
        self.pull_dataset(id = registry_ref['id'])

    def list_datasets(self) -> pd.DataFrame:
        
        dslist = self.registry.db.list_datasets()
        return pd.DataFrame(dslist)

    def bind_deployable(
            self, 
            deployable, 
            name: str, 
            target: str, 
            target_mapping: dict[str, Any] | None = None, 
            description: str = ""
    ) -> None:

        deployable = create_deployable(
            deployable=deployable, 
            dataset=self.dataset, 
            name = name, 
            target = target, 
            target_mapping=target_mapping, 
            description=description
        )

        self.push_deployable(deployable)

        if deployable.metadata.dataset_registry_id != self.dataset.metadata.id:
            self.pull_dataset(id = deployable.metadata.dataset_registry_id)

    def pull_deployable(self, id: int) -> None:

        self.deployable = self.registry.pull_deployable_from_registry(id = id)

    def push_deployable(self, deployable: Deployable) -> None:
        # pushing also pulls back the deployable, to populate proper references
        registry_ref = self.registry.push_deployable_to_registry(deployable = deployable)
        self.pull_deployable(id = registry_ref['id'])

    def list_deployables(self) -> pd.DataFrame:

        modlist = self.registry.db.list_deployables()
        return pd.DataFrame(modlist)
    
    def predict(self, input: list[dict] | pd.DataFrame, log: bool = False):

        if isinstance(input, pd.DataFrame):
            input = input.to_dict('records')

        output = self.deployable.predict(input)

        # log inputs + result
        if log:
            self.registry.log_execution(
                deployable_id=self.deployable.metadata.id, 
                input=input, 
                output=output
            )

        return output

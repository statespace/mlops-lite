import pandas as pd
from typing import Any

from mlopslite.artifacts.dataset import DataSet
from mlopslite.registry.registry import Registry
from mlopslite.registry.registryconfig import RegistryConfig
from mlopslite.artifacts.model import Deployable

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

    def bind_dataset(self, data, name: str, description: str = ""):
        dataset = DataSet.create(data = data, name = name, description=description)
        self.push_dataset(dataset=dataset)

    def pull_dataset(self, id: int) -> None:
        self.dataset = self.registry.pull_dataset_from_registry(id = id)

    def push_dataset(self, dataset: DataSet) -> None:
        # pushing also pulls back the dataset, to populate proper references
        registry_ref = self.registry.push_dataset_to_registry(dataset=dataset)
        self.pull_dataset(id = registry_ref['id'])

    def list_datasets(self) -> pd.DataFrame:
        
        dslist = self.registry.db.list_datasets()
        return pd.DataFrame(dslist)

    def bind_model(
            self, 
            object, 
            name: str, 
            target: str, 
            target_mapping: dict[str, Any] | None = None, 
            description: str = ""
    ) -> None:

        model = Deployable.create(
            deployable=object, 
            dataset=self.dataset, 
            name = name, 
            target = target, 
            target_mapping=target_mapping, 
            description=description
        )

        self.push_model(model)

        if model.metadata.dataset_registry_id != self.dataset.metadata.id:
            self.pull_dataset(id = model.metadata.dataset_registry_id)

    def pull_model(self, id: int) -> None:

        self.model = self.registry.pull_model_from_registry(id = id)

    def push_model(self, deployable: Deployable) -> None:
        # pushing also pulls back the model, to populate proper references
        registry_ref = self.registry.push_model_to_registry(deployable = deployable)
        self.pull_model(id = registry_ref['id'])

    def list_models(self) -> pd.DataFrame:

        modlist = self.registry.db.list_models()
        return pd.DataFrame(modlist)
    
    def predict(self, input: list[dict] | pd.DataFrame, log: bool = False):

        if isinstance(input, pd.DataFrame):
            input = input.to_dict('records')

        output = self.model.predict(input)

        # log inputs + result
        if log:
            pass

        return output

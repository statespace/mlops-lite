import pandas as pd

from mlopslite.artifacts.dataset import DataSet, convert_df_to_dict
from mlopslite.registry.registry import Registry
from mlopslite.artifacts.metadata import DataSetMetadata


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

    def __init__(self) -> None:
        # set up DB object
        self.registry = Registry()  # default to sqlite, workspace folder sqlite/mlops-lite.db

    def bind_dataset(self, id: int) -> None:
        self.dataset = DataSet()

    def register_dataset(self, dataset, target) -> None:
        self.dataset.register(dataset, target)

    def list_datasets(self) -> pd.DataFrame:
        return DataSet.list(self.db)

def dataset_from_registry(registry: Registry, id: int) -> DataSet:
    return registry.pull_dataset_from_registry(id=id)


def dataset_from_object(
    registry: Registry, obj: pd.DataFrame | dict, name: str, description: str = ""
) -> DataSet:
    data = pd.DataFrame(obj)

    metadata = DataSetMetadata.create(
        data=data,
        name=name,
        version=registry.db.get_dataset_version_increment(name),
        description=description,
    )

    json_data = convert_df_to_dict(data, metadata)

    registry_ref = registry.push_dataset_to_registry(data=json_data, metadata=metadata)

    return dataset_from_registry(registry=registry, id=registry_ref["id"])


def dataset_from_csv(
    registry: Registry, path: str, name: str, description: str = ""
) -> DataSet:
    data = pd.read_csv(path)

    return dataset_from_object(
        registry=registry, obj=data, name=name, description=description
    )

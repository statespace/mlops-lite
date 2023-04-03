import pandas as pd

from .artifacts.dataset import DataSet
from .artifacts.pipeline import ModelPipe
from .registry.db import DataBase


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
        self.db = DataBase()  # default to sqlite, workspace folder sqlite/mlops-lite.db

    def bind_dataset(self, id: int) -> None:
        self.dataset = DataSet()

    def register_dataset(self, dataset, target) -> None:
        self.dataset.register(dataset, target)

    def list_datasets(self) -> pd.DataFrame:
        return DataSet.list(self.db)

    def register_model(self, pipeline: ModelPipe) -> None:
        pass

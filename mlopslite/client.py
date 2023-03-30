from registry.db import DataBase
from artifacts.dataset import DataSet

class MlopsLite:

    """
    Main class that implements control over the registry - pushing and pulling artifacts
    """

    def __init__(self) -> None:

        # set up DB object
        self.db = DataBase() # default to sqlite, workspace folder sqlite/mlops-lite.db
        self.dataset = DataSet()

    def bind_dataset(self, id) -> None:
        self.dataset.bind(id)

    def register_dataset(self, dataset, target) -> None:
        self.dataset.register(dataset, target)

    def list_datasets(self) -> dict:
        pass

    



from dataclasses import dataclass
import json
from hashlib import md5

import numpy as np
import pandas as pd

from mlopslite.artifacts.metadata import DataSetMetadata

@dataclass
class DataSet:

    """
    Object containing data and reference to Registry. Which in turn means, that object can be created only through pulling a valid DataSet entry from Registry.
    Creation (effectively - registration) of new DataSet from file or object reference will first push it to Registry and pull to create proper object.
    """

    data: pd.DataFrame
    metadata: DataSetMetadata


    def convert_to_dict(self) -> dict:
        nonetypes = [np.nan, None]
        conv_dict = self.data.to_dict("list")

        column_meta = [i.__dict__ for i in self.metadata.column_metadata]

        out = {}
        for k, v in conv_dict.items():
            
            target_type = eval([i['converted_dtype'] for i in column_meta if i['column_name'] == k][0])
            out[k] = [target_type(i) if i not in nonetypes else None for i in v]

        return out
    
    @staticmethod
    def create(
        data: pd.DataFrame | dict, 
        name: str, 
        description: str = ""
    ) -> 'DataSet':
        
        # version can not initialize, unless checked against existing registry
        # this happens upon pushing data to data to the Registry
        
        data = pd.DataFrame(data)

        metadata = DataSetMetadata.create(
            data=data,
            name=name,
            version=None, 
            description=description,
        )
    
        return DataSet(data=data, metadata=metadata)

    @staticmethod
    def create_from_csv(
        path: str, 
        name: str, 
        description: str = ""
    ) -> 'DataSet':
        
        data = pd.read_csv(path)

        return DataSet.create(
            data=data, name=name, description=description
        )
    
    def get_data_hash(self) -> str:

        data = self.convert_to_dict()

        return md5(
            json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
        ).hexdigest()
    
    def info(self) -> pd.DataFrame:

        return pd.DataFrame(self.metadata.column_metadata)

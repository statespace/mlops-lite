from dataclasses import dataclass
from typing import Any

import pandas as pd

@dataclass
class ColumnMetadata:
    column_name: str
    original_dtype: str
    converted_dtype: str
    null_count: int
    unique_count: int
    min_value_num: float
    max_value_num: float
    #unique_val_str: list[str] # not all DBs implement arrays, perhaps should go as many-to-one table

    @staticmethod
    def create(key, column: pd.Series) -> "ColumnMetadata":

        # summarize col
        summary = {
            "column_name": key,
            "original_dtype": str(column.dtype),
            "converted_dtype": translate_type_to_primitive(column.dtype),
            "null_count": len([i for i in column if pd.isna(i)]),
            "unique_count": len(set([i for i in column if not pd.isna(i)])),
        }

        if summary["converted_dtype"] in ["int", "float"]:
            summary["min_value_num"] = float(min([i for i in column if not pd.isna(i)]))
            summary["max_value_num"] = float(max([i for i in column if not pd.isna(i)]))
            #summary["unique_val_str"] = None
        else:
            summary["min_value_num"] = None
            summary["max_value_num"] = None
            #summary["unique_val_str"] = list(set(column))

        return ColumnMetadata(**summary)
    
def translate_type_to_primitive(dtype):
    # caveat: pandas breaks int arrays that have None values
    # it might be fixed with pandas 2.+ arrow backend
    # conv_dtypes = {k: v.kind for k, v in dict(data.dtypes).items()}

    mapper = {"i": "int", "?": "bool", "u": "int", "f": "float", "M": "int", "O": "str"}
    return mapper[dtype.kind]
    
@dataclass
class DatasetMetadata:
    name: str
    version: int | None
    id: int | None
    description: str
    size_cols: int
    size_rows: int
    column_metadata: list[ColumnMetadata]

    @staticmethod
    def create(
        data: pd.DataFrame, 
        name: str, 
        version: int, 
        description: str,
        id: int | None = None
    ) -> "DatasetMetadata":
        
        dataset_metadata = {
            "name": name,
            "version": version,
            "id": id,
            "description": description,
            "size_cols": data.shape[1],
            "size_rows": data.shape[0],
            "column_metadata": [ColumnMetadata.create(k, v) for k, v in data.items()],
        }

        return DatasetMetadata(**dataset_metadata)

@dataclass
class DeployableMetadata:

    """
    Container for the metadata of Deployable object.
    """

    id: int | None
    dataset_registry_id: int
    name: str
    version: int | None
    target: str
    target_mapping: dict | None
    description: str
    estimator_type: str
    estimator_class: str
    variables: dict[str, Any]
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from mlopslite.artifacts.column_metadata import ColumnMetadata


@dataclass
class DataSetMetadata:
    name: str
    version: int
    description: str
    size_cols: int
    size_rows: int
    original_dtypes: dict[str, Callable]
    converted_dtypes: dict[str, Callable]
    column_metadata: dict[str, ColumnMetadata]


def create_dataset_metadata(
    data: pd.DataFrame, name: str, description: str
) -> DataSetMetadata:
    dataset_metadata = {
        "name": name,
        "version": get_dataset_version(name),
        "description": description,
        "size_cols": data.shape[0],
        "size_rows": data.shape[1],
        "original_dtypes": {k: str(v.type) for k, v in dict(data.dtypes).items()},
        "converted_dtypes": translate_type_to_primitive(data=data),
        "column_metadata": {
            k: ColumnMetadata.create(v.to_list()) for k, v in data.items()
        },
    }

    return DataSetMetadata(**dataset_metadata)


def get_dataset_version(name: str):
    pass


def translate_type_to_primitive(data: pd.DataFrame) -> dict:
    # caveat: pandas breaks int arrays that have None values
    # it might be fixed with pandas 2.+ arrow backend
    conv_dtypes = {k: v.kind for k, v in dict(data.dtypes).items()}

    mapper = {"i": int, "?": bool, "u": int, "f": float, "M": int, "O": str}
    return {k: mapper[v] for k, v in conv_dtypes.items()}

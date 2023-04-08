import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from mlopslite.artifacts.dataset_metadata import (DataSetMetadata,
                                                  create_dataset_metadata)
from mlopslite.registry.registry import Registry


@dataclass
class DataSet:

    """
    Object containing data and reference to Registry. Which in turn means, that object can be created only through pulling a valid DataSet entry from Registry.
    Creation (effectively - registration) of new DataSet from file or object reference will first push it to Registry and pull to create proper object.
    """

    data: pd.DataFrame
    metadata: DataSetMetadata
    # registry: Registry


def dataset_from_registry(registry: Registry, id: int) -> DataSet:
    return registry.pull_dataset_from_registry(id=id)


def dataset_from_object(
    registry: Registry, obj: pd.DataFrame | dict, name: str, description: str = ""
) -> DataSet:
    data = pd.DataFrame(obj)

    metadata = create_dataset_metadata(data=data, name=name, description=description)

    json_data = convert_df_to_json(data, metadata)

    id = registry.push_dataset_to_registry(data=json_data, metadata=metadata)

    return dataset_from_registry(registry=registry, id=id)


def dataset_from_csv(
    registry: Registry, path: str, name: str, description: str = ""
) -> DataSet:
    data = pd.read_csv(path)

    return dataset_from_object(
        registry=registry, obj=data, name=name, description=description
    )


def convert_df_to_json(data: pd.DataFrame, metadata: DataSetMetadata) -> str:
    nonetypes = [np.nan, None]
    conv_dict = data.to_dict("list")

    # check if match cols
    if not all([i in conv_dict.keys() for i in metadata.converted_dtypes]):
        raise Exception("Mismatch in columns!")

    out = {}
    for k, v in conv_dict.items():
        target_type = metadata.converted_dtypes[k]
        out[k] = [target_type(i) if i not in nonetypes else None for i in v]

    out = json.dumps(out, sort_keys=True, indent=2)

    return out

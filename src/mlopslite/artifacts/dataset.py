from dataclasses import dataclass

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


def convert_df_to_dict(data: pd.DataFrame, metadata: DataSetMetadata) -> dict:
    nonetypes = [np.nan, None]
    conv_dict = data.to_dict("list")

    column_meta = [i.__dict__ for i in metadata.column_metadata]

    #print(column_meta)

    out = {}
    for k, v in conv_dict.items():
        
        target_type = eval([i['converted_dtype'] for i in column_meta if i['column_name'] == k][0])
        out[k] = [target_type(i) if i not in nonetypes else None for i in v]

    # out = json.dumps(out, sort_keys=True, indent=2)

    return out

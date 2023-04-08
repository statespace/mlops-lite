import logging
import sys

import pandas as pd

from mlopslite.artifacts import dataset

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

testcase1 = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5],
        "b": [1.1, 2.1, 2.2, 2.3, 2.4],
        "c": ["a", "b", "c", "d", None],
        "d": [1, None, None, None, 2],
        "t": [0, 1, 1, 1, 0],
    }
)


class TestPytest:
    # def test_validate_dataset(self):
    #    assert dataset.(testcase1) == True

    def test_create_metadata(self):
        metadata = dataset.create_dataset_metadata(df=testcase1, target="t")
        assert isinstance(metadata, dict) == True
        assert ("original_dtypes" in list(metadata.keys())) == True

    def test_type_detection(self):
        # metadata = dataset._create_dataset_metadata(df=testcase1, target="t")
        typeconv = dataset.translate_type_to_primitive(testcase1)
        logging.info(typeconv)
        expected_types = {"a": int, "b": float, "c": str, "d": float, "t": int}
        assert expected_types == typeconv

    def test_apply_conversion(self):
        metadata = dataset.create_dataset_metadata(df=testcase1, target="t")
        conv_data = dataset.convert_df_to_dict(df=testcase1, metadata=metadata)

        assert isinstance(conv_data, dict) == True

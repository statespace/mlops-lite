import pandas as pd
import pytest

from mlopslite.artifacts.dataset import DataSet

testcase1 = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5],
        "b": [1.1, 2.1, 2.2, 2.3, 2.4],
        "c": ["a", "b", "c", "d", None],
        "d": [1, None, None, None, 2],
        "t": [0, 1, 1, 1, 0],
    }
)

dataset = DataSet()


class TestPytest:
    def test_validate_dataset(self):
        assert dataset._check_valid(testcase1) == True

    def test_create_metadata(self):
        metadata = dataset._create_dataset_metadata(df=testcase1, target="t")
        assert isinstance(metadata, dict) == True
        assert ("original_dtypes" in list(metadata.keys())) == True

    def test_type_conversion(self):
        # metadata = dataset._create_dataset_metadata(df=testcase1, target="t")
        typeconv = dataset._type_conversion(testcase1)
        # assert 1 == 1
        expected_types = {"a": int, "b": float, "c": str, "d": float, "t": int}
        assert expected_types == typeconv

    def test_apply_conversion(self):
        metadata = dataset._create_dataset_metadata(df=testcase1, target="t")
        conv_data = dataset._apply_conversion(testcase1, metadata)

        assert isinstance(conv_data, dict) == True

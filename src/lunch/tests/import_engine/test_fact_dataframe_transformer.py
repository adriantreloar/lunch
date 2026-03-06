import pandas as pd
from numpy import dtype
from pandas import DataFrame

from src.lunch.import_engine.transformers.fact_dataframe_transformer import (
    FactDataFrameTransformer,
)


def test_rename_columns():
    df = DataFrame([{"dept": 1, "sales": 10.0}])
    result = FactDataFrameTransformer.rename(df, {"dept": 3, "sales": 1})
    assert list(result.columns) == [3, 1]
    assert result[3].tolist() == [1]
    assert result[1].tolist() == [10.0]


def test_rename_partial_mapping():
    df = DataFrame([{"a": 1, "b": 2, "c": 3}])
    result = FactDataFrameTransformer.rename(df, {"a": 10, "b": 20})
    assert set(result.columns) == {10, 20, "c"}


def test_column_types_from_mapping():
    result = FactDataFrameTransformer.column_types_from_mapping({"dept": 3, "sales": 1})
    assert result == {3: dtype(str), 1: dtype(str)}


def test_column_types_from_mapping_empty():
    assert FactDataFrameTransformer.column_types_from_mapping({}) == {}

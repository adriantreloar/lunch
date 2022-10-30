import numpy as np
import pytest
from pandas import DataFrame

from lunch.reference_data.transformers.dimension_dataframe_transformer import (
    DimensionDataFrameTransformer,
)


@pytest.mark.asyncio
async def test_make_dataframe():

    # async def to_async_gen(iterable):
    #    for i in iterable:
    #        yield i
    #        await asyncio.sleep(0)

    columns = {-1: range(5), 0: ["foo", "bar", "baz", "guf", "gof"], 2: [5, 4, 3, 2, 1]}
    dtypes = {-1: np.int, 0: np.object, 2: np.int}

    result = DimensionDataFrameTransformer.make_dataframe(
        columns=columns, dtypes=dtypes
    )

    assert isinstance(result, DataFrame)
    assert len(result.columns) == 3
    assert result[-1].tolist() == [0, 1, 2, 3, 4]
    assert result[0].tolist() == ["foo", "bar", "baz", "guf", "gof"]
    assert result[2].tolist() == [5, 4, 3, 2, 1]


def test_basic_merge():

    compare_df = DataFrame([{"a": "foo", "b": 10}, {"a": "bar", "b": 20}])

    source_df = DataFrame([{"a": "bar", "b": 21}, {"a": "baz", "b": 30}])

    key = ["a"]

    result = DimensionDataFrameTransformer.merge(
        source_df=source_df, compare_df=compare_df, key=key
    )

    # NOTE - the result has been sorted by key
    expected = DataFrame(
        [
            {"a": "bar", "b": 21},
            {"a": "baz", "b": 30},
            {"a": "foo", "b": 10},
        ]
    )

    assert (expected["a"] == result["a"]).all(), (expected, result)
    assert (expected["b"] == result["b"]).all(), (expected, result)


def test_merge_with_int_columns():

    compare_df = DataFrame([{1: "foo", 2: 10}, {1: "bar", 2: 20}])

    source_df = DataFrame([{1: "bar", 2: 21}, {1: "baz", 2: 30}])

    key = [1]

    result = DimensionDataFrameTransformer.merge(
        source_df=source_df, compare_df=compare_df, key=key
    )

    # NOTE - the result has been sorted by key
    expected = DataFrame(
        [
            {1: "bar", 2: 21},
            {1: "baz", 2: 30},
            {1: "foo", 2: 10},
        ]
    )

    assert (expected[1] == result[1]).all(), (expected, result)
    assert (expected[2] == result[2]).all(), (expected, result)


def test_merge_with_id_column():

    compare_df = DataFrame([{-1: 0, 1: "foo", 2: 10}, {-1: 1, 1: "bar", 2: 20}])

    source_df = DataFrame([{1: "bar", 2: 21}, {1: "baz", 2: 30}])

    key = [1]

    result = DimensionDataFrameTransformer.merge(
        source_df=source_df, compare_df=compare_df, key=key
    )

    # NOTE - the result has been sorted by key
    expected = DataFrame(
        [
            {-1: 1, 1: "bar", 2: 21},
            {-1: np.NaN, 1: "baz", 2: 30},
            {-1: 0, 1: "foo", 2: 10},
        ]
    )

    assert (expected[-1].fillna(-1) == result[-1].fillna(-1)).all(), (expected, result)
    assert (expected[1] == result[1]).all(), (expected, result)
    assert (expected[2] == result[2]).all(), (expected, result)


@pytest.mark.asyncio
async def test_columnize():
    # dictionary of columns? attribute_id : column/iterator
    # index is -1?
    # columnize(data: pd.DataFrame) -> dict[int: Iterable]

    source_df = DataFrame(
        [
            {-1: 1, 1: "bar", 2: 21},
            {-1: np.NaN, 1: "baz", 2: 30},
            {-1: 0, 1: "foo", 2: 10},
        ]
    )

    result = DimensionDataFrameTransformer.columnize(data=source_df)

    assert list(result[-1])[0] == 1, result[-1]
    assert np.isnan(list(result[-1])[1]), result[-1]
    assert list(result[-1])[2] == 0, result[-1]

    assert list(result[1]) == ["bar", "baz", "foo"], result[1]
    assert list(result[2]) == [21, 30, 10], result[2]

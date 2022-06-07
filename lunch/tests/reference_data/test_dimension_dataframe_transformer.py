from lunch.reference_data.transformers.dimension_dataframe_transformer import DimensionDataFrameTransformer
from pandas import DataFrame
import numpy as np
import pytest

@pytest.mark.asyncio
async def test_make_dataframe():

    #async def to_async_gen(iterable):
    #    for i in iterable:
    #        yield i
    #        await asyncio.sleep(0)

    columns = {-1: range(5),
               0: ["foo", "bar", "baz", "guf", "gof"],
               2: [5, 4, 3, 2, 1]
               }
    dtypes = {-1: np.int, 0: np.object, 2: np.int}

    result = DimensionDataFrameTransformer.make_dataframe(columns=columns, dtypes=dtypes)

    assert isinstance(result, DataFrame)
    assert len(result.columns) == 3
    assert result[-1].tolist() == [0,1,2,3,4]
    assert result[0].tolist() == ["foo","bar","baz","guf","gof"]
    assert result[2].tolist() == [5,4,3,2,1]


@pytest.mark.asyncio
async def test_basic_merge():

    compare_df = DataFrame([{"a": "foo", "b": 10}, {"a": "bar", "b": 20}])

    source_df = DataFrame([{"a": "bar", "b": 21}, {"a": "baz", "b": 30}])

    key = ["a"]

    result = DimensionDataFrameTransformer.merge(source_df=source_df,
                                                compare_df=compare_df,
                                                key=key)

    # NOTE - the result has been sorted by key
    expected = DataFrame([{"a": "bar", "b": 21},
                          {"a": "baz", "b": 30},
                          {"a": "foo", "b": 10},
                          ])

    assert (expected["a"] == result["a"]).all(), (expected, result)
    assert (expected["b"] == result["b"]).all(), (expected, result)

@pytest.mark.asyncio
async def test_columnize():

    assert False

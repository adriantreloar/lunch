import numpy as np
import pytest
from mock import Mock

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.null_dimension_data_cache import NullDimensionDataCache
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.serialization.columnar_dimension_data_serializer import (
    ColumnarDimensionDataSerializer,
)


@pytest.fixture()
def null_cache_dimension_data_store():

    # Don't mock transformers

    serializer = Mock(ColumnarDimensionDataSerializer)
    cache = NullDimensionDataCache()

    testee_dimension_data_store = DimensionDataStore(
        serializer=serializer,
        cache=cache,
    )
    yield testee_dimension_data_store, serializer, cache


d_test = {
    "name": "Test",
    "attributes": [
        {"name": "foo"},
        {"name": "bar"},
        {"name": "baz"},
    ],
    "key": [
        "foo",
    ],
}
d_foo = {
    "name": "Foo",
    "attributes": [
        {"name": "foo"},
    ],
    "key": [
        "foo",
    ],
}

v0 = Version(
    version=0,
    model_version=0,
    reference_data_version=0,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)
v1 = Version(
    version=1,
    model_version=1,
    reference_data_version=1,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)
v2 = Version(
    version=2,
    model_version=2,
    reference_data_version=0,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)


@pytest.mark.parametrize(
    "test_input,test_setup,expected_result",
    [
        pytest.param(
            {"dimension_id": 1, "read_version": v1, "column_types": {}},
            {"read_version_index": {}, "redirect_version": v1, "read_columns": None},
            {"expected_columns": None},
            id="no_dimension_data_exists",
        ),
        pytest.param(
            {
                "dimension_id": 1,
                "read_version": v1,
                "column_types": {0: np.dtype(str), 1: np.dtype(str)},
            },
            {
                "read_version_index": {1: 1},
                "redirect_version": v1,
                "read_columns": {0: ["Foo", "Bar"], 1: ["Do", "Re"]},
            },
            {
                "expected_columns": {0: ["Foo", "Bar"], 1: ["Do", "Re"]},
            },
            id="dimension_data_exists_at_version",
        ),
        pytest.param(
            {
                "dimension_id": 1,
                "read_version": v2,
                "column_types": {0: np.dtype(str), 1: np.dtype(str)},
            },
            {
                "read_version_index": {1: 1},
                "redirect_version": v1,
                "read_columns": {0: ["Foo", "Bar"], 1: ["Do", "Re"]},
            },
            {
                "expected_columns": {0: ["Foo", "Bar"], 1: ["Do", "Re"]},
            },
            id="dimension_data_exists_via_index",
        ),
    ],
)
async def test_get_dimension_columns(
    null_cache_dimension_data_store, test_input, test_setup, expected_result
):

    testee_dimension_data_store, serializer, _ = null_cache_dimension_data_store

    dimension_id = test_input["dimension_id"]
    read_version = test_input["read_version"]
    column_types = test_input["column_types"]

    read_version_index = test_setup["read_version_index"]
    read_columns = test_setup["read_columns"]
    redirect_version = test_setup["redirect_version"]

    expected_columns = expected_result["expected_columns"]

    serializer.get_version_index.return_value = read_version_index

    if read_columns is None:
        serializer.get_columns.side_effect = KeyError
        # If we don't expect a dimension id to be there, we should get a key error
        with pytest.raises(KeyError):
            print(
                await testee_dimension_data_store.get_columns(
                    read_version=read_version,
                    dimension_id=dimension_id,
                    column_types=column_types,
                    filter=None,
                )
            )
    else:
        serializer.get_columns.return_value = read_columns

        returned_columns = await testee_dimension_data_store.get_columns(
            read_version=read_version,
            dimension_id=dimension_id,
            column_types=column_types,
            filter=None,
        )

        assert expected_columns == returned_columns

        serializer.get_columns.assert_called_with(
            dimension_id=dimension_id,
            reference_data_version=redirect_version.reference_data_version,
            column_types=column_types,
        )

    serializer.get_version_index.assert_called_with(version=read_version)


# @pytest.mark.parametrize(
#    "test_input,test_setup,expected_result",
#    [
#        pytest.param(
#            {"put_dimensions": [d_test], "read_version": v1, "write_version": v2},
#            {
#                "read_version_index": {},
#                "read_name_index": {},
#                "read_max_dimension_id": 0,
#                "read_dimensions": {},
#            },
#            {
#                "written_dimensions": [{**d_test, **{"id_": 1, "model_version": 2}}],
#                "written_name_index": {"Test": 1},
#                "written_version_index": {1: 2},  # dimension 1 is at version 2
#            },
#            id="initial_insert_of_any_dimension",
#        ),
#        pytest.param(
#            {"put_dimensions": [d_test], "read_version": v1, "write_version": v2},
#            {
#                "read_version_index": {1: 1},  # dimension 1 is at version 1
#                "read_name_index": {"Foo": 1},
#                "read_max_dimension_id": 1,
#                "read_dimensions": {},
#            },
#            {
#                "written_dimensions": [{**d_test, **{"id_": 2, "model_version": 2}}],
#                "written_name_index": {"Foo": 1, "Test": 2},
#                "written_version_index": {
#                    1: 1,
#                    2: 2,
#                },  # dimension 1 is STILL at version 1, dim 2 (d_test) at version 2
#            },
#            id="insert_where_other_dimensions_exist",
#        ),
#        pytest.param(
#            {
#                "put_dimensions": [
#                    {**d_test, **{"id_": 1, "model_version": 1, "name": "Test_Update"}}
#                ],
#                "read_version": v1,
#                "write_version": v2,
#            },
#            {
#                "read_version_index": {1: 1},  # dimension 1 is at version 1
#                "read_name_index": {"Test": 1},
#                "read_max_dimension_id": 1,
#                "read_dimensions": {
#                    (1, 1): {**d_test, **{"id_": 1, "model_version": 1}}
#                },
#            },
#            {
#                "written_dimensions": [
#                    {**d_test, **{"id_": 1, "model_version": 2, "name": "Test_Update"}}
#                ],
#                "written_name_index": {"Test_Update": 1},
#                "written_version_index": {1: 2},  # dimension 1 is at version 2
#            },
#            id="update_single_dimension",
#        ),
#    ],
# )
# async def test_put_dimensions(
#    null_cache_model_store, test_setup, test_input, expected_result
# ):
#
#    testee_model_store, serializer, _ = null_cache_model_store
#
#    put_dimensions = test_input["put_dimensions"]
#    read_version = test_input["read_version"]
#    write_version = test_input["write_version"]
#
#    read_version_index = test_setup["read_version_index"]
#    read_name_index = test_setup["read_name_index"]
#    read_max_dimension_id = test_setup["read_max_dimension_id"]
#
#    # Model version and id have been added to dimension
#    written_dimensions = expected_result["written_dimensions"]
#    written_name_index = expected_result["written_name_index"]
#    written_version_index = expected_result["written_version_index"]
#
#    serializer.get_dimension_version_index.return_value = read_version_index
#    serializer.get_dimension_name_index.return_value = read_name_index
#    serializer.get_max_dimension_id.return_value = read_max_dimension_id
#
#    def dimension_id_returns(id_: int, model_version: int):
#        return test_setup["read_dimensions"][(id_, model_version)]
#
#    serializer.get_dimension.side_effect = dimension_id_returns
#
#    await testee_model_store.put_dimensions(
#        dimensions=put_dimensions,
#        read_version=read_version,
#        write_version=write_version,
#    )
#
#    # Check that all of the gets have been called with the read version
#    serializer.get_max_dimension_id.assert_called_with(version=read_version)
#    serializer.get_dimension_version_index.assert_called_with(version=read_version)
#    serializer.get_dimension_name_index.assert_called_with(version=read_version)
#    for id_, model_version in test_setup["read_dimensions"].keys():
#        serializer.get_dimension.assert_called_with(
#            model_version=model_version, id_=id_
#        )
#
#    # Check that all of the puts have been put with the write version
#    serializer.put_dimension_version_index.assert_called_with(
#        index_=written_version_index, version=write_version
#    )
#    serializer.put_dimension_name_index.assert_called_with(
#        index_=written_name_index, version=write_version
#    )
#    serializer.put_dimensions.assert_called_with(
#        dimensions=written_dimensions, model_version=write_version.model_version
#    )

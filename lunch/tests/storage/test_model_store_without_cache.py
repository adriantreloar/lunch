import pytest
from mock import Mock

from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_comparer import FactComparer
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.version import Version
from lunch.storage.cache.null_model_cache import NullModelCache
from lunch.storage.model_store import ModelStore
from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
)
from lunch.storage.transformers.fact_model_index_transformer import FactModelIndexTransformer


@pytest.fixture()
def null_cache_model_store():

    # Don't mock these transformers
    dimension_transformer = DimensionTransformer()
    dimension_index_transformer = DimensionModelIndexTransformer()
    dimension_comparer = DimensionComparer()
    fact_transformer = FactTransformer()
    fact_index_transformer = FactModelIndexTransformer()
    fact_comparer = FactComparer()

    serializer = Mock(ModelSerializer)
    cache = NullModelCache()

    testee_model_store = ModelStore(
        dimension_transformer=dimension_transformer,
        dimension_index_transformer=dimension_index_transformer,
        dimension_comparer=dimension_comparer,
        fact_transformer=fact_transformer,
        fact_index_transformer=fact_index_transformer,
        fact_comparer=fact_comparer,
        serializer=serializer,
        cache=cache,
    )
    yield testee_model_store, serializer, cache


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
    reference_data_version=0,
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
            {"dimension_name": "Test", "read_version": v1},
            {"read_name_index": {}},
            {"returned_dimension_id": None},
            id="no_dimensions_exist",
        ),
        pytest.param(
            {"dimension_name": "Test", "read_version": v1},
            {"read_name_index": {"Foo": 1, "Bar": 2}},
            {"returned_dimension_id": None},
            id="other_dimensions_exist",
        ),
        pytest.param(
            {"dimension_name": "Test", "read_version": v1},
            {"read_name_index": {"Test": 1}},
            {"returned_dimension_id": 1},
            id="dimension_exists_alone",
        ),
        pytest.param(
            {"dimension_name": "Test", "read_version": v1},
            {"read_name_index": {"Foo": 1, "Test": 2, "Bar": "3"}},
            {"returned_dimension_id": 2},
            id="dimension_exists_with_others",
        ),
    ],
)
async def test_get_dimension_id(
    null_cache_model_store, test_setup, test_input, expected_result
):

    testee_model_store, serializer, _ = null_cache_model_store

    dimension_name = test_input["dimension_name"]
    read_version = test_input["read_version"]

    read_name_index = test_setup["read_name_index"]

    expected_dimension_id = expected_result["returned_dimension_id"]

    serializer.get_dimension_name_index.return_value = read_name_index

    if expected_dimension_id is None:

        # If we don't expect a dimension id to be there, we should get a key error
        with pytest.raises(KeyError):
            await testee_model_store.get_dimension_id(
                name=dimension_name, version=read_version
            )
    else:

        returned_dimension_id = await testee_model_store.get_dimension_id(
            name=dimension_name, version=read_version
        )

        assert expected_dimension_id == returned_dimension_id


@pytest.mark.parametrize(
    "test_input,test_setup,expected_result",
    [
        pytest.param(
            {"dimension_id": 1, "read_version": v1},
            {"read_version_index": {}, "redirect_version": v1, "read_dimension": None},
            {"expected_dimension": None},
            id="no_dimensions_exist",
        ),
        pytest.param(
            {"dimension_id": 1, "read_version": v1},
            {
                "read_version_index": {1: 1},
                "redirect_version": v1,
                "read_dimension": {**d_test, **{"id_": 1, "model_version": 1}},
            },
            {"expected_dimension": {**d_test, **{"id_": 1, "model_version": 1}}},
            id="dimension_exists_at_version",
        ),
        pytest.param(
            {"dimension_id": 1, "read_version": v2},
            {
                "read_version_index": {1: 1},
                "redirect_version": v1,
                "read_dimension": {**d_test, **{"id_": 1, "model_version": 1}},
            },
            {"expected_dimension": {**d_test, **{"id_": 1, "model_version": 1}}},
            id="dimension_exists_via_index",
        ),
    ],
)
async def test_get_dimension(
    null_cache_model_store, test_setup, test_input, expected_result
):

    testee_model_store, serializer, _ = null_cache_model_store

    dimension_id = test_input["dimension_id"]
    read_version = test_input["read_version"]

    read_version_index = test_setup["read_version_index"]
    read_dimension = test_setup["read_dimension"]
    redirect_version = test_setup["redirect_version"]

    expected_dimension = expected_result["expected_dimension"]

    serializer.get_dimension_version_index.return_value = read_version_index

    if read_dimension is None:
        serializer.get_dimension.side_effect = KeyError
        # If we don't expect a dimension id to be there, we should get a key error
        with pytest.raises(KeyError):
            print(
                await testee_model_store.get_dimension(
                    id_=dimension_id, version=read_version
                )
            )
    else:
        serializer.get_dimension.return_value = read_dimension

        returned_dimension = await testee_model_store.get_dimension(
            id_=dimension_id, version=read_version
        )

        assert expected_dimension == returned_dimension

        serializer.get_dimension.assert_called_with(
            id_=dimension_id, model_version=redirect_version.model_version
        )

    serializer.get_dimension_version_index.assert_called_with(version=read_version)


@pytest.mark.parametrize(
    "test_input,test_setup,expected_result",
    [
        pytest.param(
            {"put_dimensions": [d_test], "read_version": v1, "write_version": v2},
            {
                "read_version_index": {},
                "read_name_index": {},
                "read_max_dimension_id": 0,
                "read_dimensions": {},
            },
            {
                "written_dimensions": [{**d_test, **{"id_": 1, "model_version": 2}}],
                "written_name_index": {"Test": 1},
                "written_version_index": {1: 2},  # dimension 1 is at version 2
            },
            id="initial_insert_of_any_dimension",
        ),
        pytest.param(
            {"put_dimensions": [d_test], "read_version": v1, "write_version": v2},
            {
                "read_version_index": {1: 1},  # dimension 1 is at version 1
                "read_name_index": {"Foo": 1},
                "read_max_dimension_id": 1,
                "read_dimensions": {},
            },
            {
                "written_dimensions": [{**d_test, **{"id_": 2, "model_version": 2}}],
                "written_name_index": {"Foo": 1, "Test": 2},
                "written_version_index": {
                    1: 1,
                    2: 2,
                },  # dimension 1 is STILL at version 1, dim 2 (d_test) at version 2
            },
            id="insert_where_other_dimensions_exist",
        ),
        pytest.param(
            {
                "put_dimensions": [
                    {**d_test, **{"id_": 1, "model_version": 1, "name": "Test_Update"}}
                ],
                "read_version": v1,
                "write_version": v2,
            },
            {
                "read_version_index": {1: 1},  # dimension 1 is at version 1
                "read_name_index": {"Test": 1},
                "read_max_dimension_id": 1,
                "read_dimensions": {
                    (1, 1): {**d_test, **{"id_": 1, "model_version": 1}}
                },
            },
            {
                "written_dimensions": [
                    {**d_test, **{"id_": 1, "model_version": 2, "name": "Test_Update"}}
                ],
                "written_name_index": {"Test_Update": 1},
                "written_version_index": {1: 2},  # dimension 1 is at version 2
            },
            id="update_single_dimension",
        ),
    ],
)
async def test_put_dimensions(
    null_cache_model_store, test_setup, test_input, expected_result
):

    testee_model_store, serializer, _ = null_cache_model_store

    put_dimensions = test_input["put_dimensions"]
    read_version = test_input["read_version"]
    write_version = test_input["write_version"]

    read_version_index = test_setup["read_version_index"]
    read_name_index = test_setup["read_name_index"]
    read_max_dimension_id = test_setup["read_max_dimension_id"]

    # Model version and id have been added to dimension
    written_dimensions = expected_result["written_dimensions"]
    written_name_index = expected_result["written_name_index"]
    written_version_index = expected_result["written_version_index"]

    # Raise an error the first time we try to read the WRITE index
    # we'll read the read index if we aren't already in the process of writing
    is_first_time_reading_write_version = [True]

    def dimension_version_index_returns(version: Version):
        if is_first_time_reading_write_version[0] and version == write_version:
            is_first_time_reading_write_version[0] = False
            raise KeyError(version)
        return read_version_index

    is_first_time_reading_write_name_version = [True]

    def dimension_name_index_returns(version: Version):
        if is_first_time_reading_write_name_version[0] and version == write_version:
            is_first_time_reading_write_name_version[0] = False
            raise KeyError(version)
        return read_name_index

    serializer.get_dimension_version_index.side_effect = dimension_version_index_returns
    serializer.get_dimension_name_index.side_effect = dimension_name_index_returns
    serializer.get_max_dimension_id.return_value = read_max_dimension_id

    def dimension_id_returns(id_: int, model_version: int):
        return test_setup["read_dimensions"][(id_, model_version)]

    serializer.get_dimension.side_effect = dimension_id_returns

    await testee_model_store.put_dimensions(
        dimensions=put_dimensions,
        read_version=read_version,
        write_version=write_version,
    )

    # Check that all of the gets have been called with the read version
    serializer.get_max_dimension_id.assert_called_with(version=read_version)
    serializer.get_dimension_version_index.assert_called_with(version=read_version)
    serializer.get_dimension_name_index.assert_called_with(version=read_version)
    for id_, model_version in test_setup["read_dimensions"].keys():
        serializer.get_dimension.assert_called_with(
            model_version=model_version, id_=id_
        )

    # Check that all of the puts have been put with the write version
    serializer.put_dimension_version_index.assert_called_with(
        index_=written_version_index, version=write_version
    )
    serializer.put_dimension_name_index.assert_called_with(
        index_=written_name_index, version=write_version
    )
    serializer.put_dimensions.assert_called_with(
        dimensions=written_dimensions, model_version=write_version.model_version
    )

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
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)
from lunch.storage.transformers.fact_index_transformer import FactIndexTransformer


@pytest.fixture()
def null_cache_model_store():

    # Don't mock these transformers
    dimension_transformer = DimensionTransformer()
    dimension_index_transformer = DimensionIndexTransformer()
    dimension_comparer = DimensionComparer()
    fact_transformer = FactTransformer()
    fact_index_transformer = FactIndexTransformer()
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
            id="initial_insert_any_dimension",
        ),
        # TODO test dimensions with and without ids
        # TODO test updates to dimension
    ],
)
async def test_put_dimensions_first_insert(
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

    serializer.get_dimension_version_index.return_value = read_version_index
    serializer.get_dimension_name_index.return_value = read_name_index
    serializer.get_max_dimension_id.return_value = read_max_dimension_id

    def dimension_id_returns(id_: int, version: Version):
        return test_setup["read_dimensions"][(id_, version)]

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
    for id_, version in test_setup["read_dimensions"].keys():
        serializer.get_dimension.assert_called_with(version=version, dimension_id=id_)

    # Check that all of the puts have been put with the write version
    serializer.put_dimension_version_index.assert_called_with(
        index_=written_version_index, version=write_version
    )
    serializer.put_dimension_name_index.assert_called_with(
        index_=written_name_index, version=write_version
    )
    serializer.put_dimensions.assert_called_with(
        dimensions=written_dimensions, version=write_version
    )

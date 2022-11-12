import pytest
from mock import Mock

from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_comparer import FactComparer
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache
from lunch.storage.cache.null_model_cache import NullModelCache
from lunch.storage.model_store import ModelStore
from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)
from lunch.storage.transformers.fact_index_transformer import FactIndexTransformer


@pytest.mark.asyncio
async def test_put_indexes_when_putting_dimensions():

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

    read_version = Version(
        version=1,
        model_version=1,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )
    write_version = Version(
        version=2,
        model_version=2,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )

    # TODO test dimensions with and without ids
    # TODO test updates to dimension

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

    serializer.get_dimension_version_index.return_value = {1: 1}
    serializer.get_dimension_name_index.return_value = {}
    serializer.get_max_dimension_id.return_value = 0

    await testee_model_store.put_dimensions(
        dimensions=[d_test], read_version=read_version, write_version=write_version
    )

    # Check that all of the gets have been called with the read version
    serializer.get_max_dimension_id.assert_called_with(version=read_version)
    serializer.get_dimension_version_index.assert_called_with(version=read_version)
    serializer.get_dimension_name_index.assert_called_with(version=read_version)

    serializer.put_dimension_version_index.assert_called_with(
        index_={1: 2}, version=write_version
    )
    serializer.put_dimension_name_index.assert_called_with(
        index_={"Test": 1}, version=write_version
    )

    # NOTE: model version and id have been added to dimension
    written_dimension = {
        "id_": 1,
        "model_version": 2,
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
    serializer.put_dimensions.assert_called_with(
        dimensions=[written_dimension], version=write_version
    )

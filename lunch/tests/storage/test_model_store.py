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

    serializer.get_dimension_version_index.return_value = {0: 0, 1: 1}
    serializer.get_dimension_name_index.return_value = {}

    serializer.get_max_dimension_id.return_value = 0

    await testee_model_store.put_dimensions(
        dimensions=[d_test], read_version=read_version, write_version=write_version
    )

    serializer.get_max_dimension_id.assert_called_with(version=read_version)

    # TODO test dimensions with and without ids
    # dimension_transformer.get_id_from_dimension(dimension)

    # TODO Check for changes
    # dimension_names_with_changes = list(dimensions_without_ids.keys())
    #    previous_dimension = await ms._get_dimension(
    #        id_=id_, version=read_version, serializer=serializer, cache=cache
    #    )

    # CHECK
    # dimension_transformer.add_model_version_to_dimension(

    # CHECK
    # Mock calls to serializer
    # dimensions_version_index_read = await _get_dimension_version_index(
    #    version=read_version, serializer=serializer, cache=cache
    # )
    # dimensions_name_index_read = await _get_dimension_name_index(
    #    version=read_version, serializer=serializer, cache=cache
    # )

    # CHECK
    # Mock calls to serializer
    # await _put_dimension_version_index(
    #    index_=dimensions_version_index_write,
    #    version=write_version,
    #    serializer=serializer,
    #    cache=cache,
    # )
    # await _put_dimension_name_index(
    #    index_=dimensions_name_index_write,
    #    version=write_version,
    #    serializer=serializer,
    #    cache=cache,
    # )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    # CHECK
    # await serializer.put_dimensions(dimensions_with_ids_and_versions, write_version)

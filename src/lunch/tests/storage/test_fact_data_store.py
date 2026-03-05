import pytest
from mock import Mock
from numpy import dtype

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.null_fact_data_cache import NullFactDataCache
from src.lunch.storage.fact_data_store import FactDataStore
from src.lunch.storage.serialization.columnar_fact_data_serializer import (
    ColumnarFactDataSerializer,
)

v0 = Version(version=0, model_version=0, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)
v1 = Version(version=1, model_version=1, reference_data_version=1,
             cube_data_version=0, operations_version=0, website_version=0)


@pytest.fixture
def null_cache_fact_data_store():
    serializer = Mock(ColumnarFactDataSerializer)
    cache = NullFactDataCache()
    store = FactDataStore(serializer=serializer, cache=cache)
    return store, serializer


# ---------------------------------------------------------------------------
# get_columns
# ---------------------------------------------------------------------------

async def test_get_columns_returns_data_looked_up_via_version_index(null_cache_fact_data_store):
    store, serializer = null_cache_fact_data_store
    # Version index maps fact_id=1 to reference_data_version=5
    serializer.get_version_index.return_value = {1: 5}
    serializer.get_columns.return_value = {3: [10, 20], 1: [1.0, 2.0]}

    result = await store.get_columns(
        read_version=v1, fact_id=1,
        column_types={3: dtype("str"), 1: dtype("str")},
        filter=None,
    )

    assert result == {3: [10, 20], 1: [1.0, 2.0]}
    serializer.get_columns.assert_called_once_with(
        dimension_id=1,
        reference_data_version=5,
        column_types={3: dtype("str"), 1: dtype("str")},
    )


async def test_get_columns_raises_key_error_when_fact_absent_from_version_index(
    null_cache_fact_data_store,
):
    store, serializer = null_cache_fact_data_store
    serializer.get_version_index.return_value = {}  # fact_id=1 not present

    with pytest.raises(KeyError):
        await store.get_columns(
            read_version=v1, fact_id=1,
            column_types={}, filter=None,
        )

    serializer.get_columns.assert_not_called()


# ---------------------------------------------------------------------------
# put
# ---------------------------------------------------------------------------

async def test_put_writes_updated_version_index_and_column_data(null_cache_fact_data_store):
    store, serializer = null_cache_fact_data_store
    serializer.get_version_index.return_value = {}
    serializer.get_partition_index.return_value = {}
    serializer.put_columns.return_value = None

    await store.put(
        fact_id=1,
        columnar_data={3: [10, 20], 1: [1.0, 2.0]},
        read_version=v0,
        write_version=v1,
    )

    # Version index should record fact_id=1 at the write reference_data_version
    serializer.put_version_index.assert_called_once()
    written_index = serializer.put_version_index.call_args.kwargs["index_"]
    assert written_index[1] == v1.reference_data_version

    # Column data should be written for fact_id=1
    serializer.put_columns.assert_called_once_with(
        dimension_id=1,
        columns={3: [10, 20], 1: [1.0, 2.0]},
        version=v1,
    )


async def test_put_propagates_version_index_write_failure(null_cache_fact_data_store):
    store, serializer = null_cache_fact_data_store
    serializer.get_version_index.return_value = {}
    serializer.put_version_index.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await store.put(
            fact_id=1,
            columnar_data={3: [10]},
            read_version=v0,
            write_version=v1,
        )

    # Column data must not have been written if the index write failed
    serializer.put_columns.assert_not_called()


async def test_put_propagates_column_write_failure(null_cache_fact_data_store):
    store, serializer = null_cache_fact_data_store
    serializer.get_version_index.return_value = {}
    serializer.put_columns.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await store.put(
            fact_id=1,
            columnar_data={3: [10]},
            read_version=v0,
            write_version=v1,
        )

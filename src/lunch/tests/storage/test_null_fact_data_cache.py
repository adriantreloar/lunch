import pytest

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.null_fact_data_cache import NullFactDataCache

v1 = Version(
    version=1, model_version=1, reference_data_version=1, cube_data_version=0, operations_version=0, website_version=0
)


@pytest.fixture
def cache():
    return NullFactDataCache()


# ---------------------------------------------------------------------------
# version index
# ---------------------------------------------------------------------------


async def test_get_version_index_raises_key_error(cache):
    with pytest.raises(KeyError):
        await cache.get_version_index(version=v1)


async def test_put_version_index_is_no_op(cache):
    await cache.put_version_index(index_={1: 5}, version=v1)  # must not raise


# ---------------------------------------------------------------------------
# partition index
# ---------------------------------------------------------------------------


async def test_get_partition_index_raises_key_error(cache):
    with pytest.raises(KeyError):
        await cache.get_partition_index(version=v1)


async def test_put_partition_index_is_no_op(cache):
    await cache.put_partition_index(index_={}, version=v1)  # must not raise


# ---------------------------------------------------------------------------
# columns
# ---------------------------------------------------------------------------


async def test_get_columns_raises_key_error(cache):
    with pytest.raises(KeyError):
        await cache.get_columns(fact_id=1, cube_data_version=1)


async def test_put_columns_is_no_op(cache):
    await cache.put_columns(fact_id=1, columns={3: [1, 2]}, version=v1)  # must not raise


# ---------------------------------------------------------------------------
# abort_write
# ---------------------------------------------------------------------------


async def test_abort_write_is_no_op(cache):
    await cache.abort_write(version=v1)  # must not raise

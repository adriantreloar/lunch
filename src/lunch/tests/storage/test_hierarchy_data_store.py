"""Tests for HierarchyDataStore."""

import pytest

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.null_hierarchy_data_cache import NullHierarchyDataCache
from src.lunch.storage.hierarchy_data_store import HierarchyDataStore
from src.lunch.storage.persistence.stringio_hierarchy_data_persistor import (
    StringIOHierarchyDataPersistor,
)
from src.lunch.storage.serialization.yaml_hierarchy_data_serializer import (
    YamlHierarchyDataSerializer,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VERSION_0 = Version(
    version=0,
    model_version=0,
    reference_data_version=0,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)

_VERSION_1 = Version(
    version=1,
    model_version=1,
    reference_data_version=1,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)

_VERSION_2 = Version(
    version=2,
    model_version=1,
    reference_data_version=2,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)


@pytest.fixture()
def store():
    persistor = StringIOHierarchyDataPersistor()
    serializer = YamlHierarchyDataSerializer(persistor=persistor)
    cache = NullHierarchyDataCache()
    return HierarchyDataStore(serializer=serializer, cache=cache)


# ---------------------------------------------------------------------------
# Round-trip: put then get_pairs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_then_get_pairs_returns_stored_pairs(store):
    pairs = [[10, 20], [10, 30], [20, 40]]
    await store.put(dimension_id=1, pairs=pairs, read_version=_VERSION_0, write_version=_VERSION_1)
    result = await store.get_pairs(dimension_id=1, read_version=_VERSION_1)
    assert result == pairs


@pytest.mark.asyncio
async def test_put_empty_pairs_round_trips(store):
    await store.put(dimension_id=2, pairs=[], read_version=_VERSION_0, write_version=_VERSION_1)
    result = await store.get_pairs(dimension_id=2, read_version=_VERSION_1)
    assert result == []


# ---------------------------------------------------------------------------
# Version isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_pairs_at_wrong_version_raises_key_error(store):
    pairs = [[1, 2]]
    await store.put(dimension_id=1, pairs=pairs, read_version=_VERSION_0, write_version=_VERSION_2)
    with pytest.raises(KeyError):
        await store.get_pairs(dimension_id=1, read_version=_VERSION_1)


@pytest.mark.asyncio
async def test_second_put_overwrites_first(store):
    pairs_v1 = [[1, 2]]
    pairs_v2 = [[1, 2], [2, 3]]
    await store.put(dimension_id=1, pairs=pairs_v1, read_version=_VERSION_0, write_version=_VERSION_1)
    await store.put(dimension_id=1, pairs=pairs_v2, read_version=_VERSION_1, write_version=_VERSION_2)
    result = await store.get_pairs(dimension_id=1, read_version=_VERSION_2)
    assert result == pairs_v2


# ---------------------------------------------------------------------------
# Unknown dimension
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_pairs_for_unknown_dimension_raises_key_error(store):
    await store.put(dimension_id=1, pairs=[[1, 2]], read_version=_VERSION_0, write_version=_VERSION_1)
    with pytest.raises(KeyError):
        await store.get_pairs(dimension_id=99, read_version=_VERSION_1)


# ---------------------------------------------------------------------------
# Multiple dimensions in same version
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_multiple_dimensions_independent(store):
    pairs_a = [[10, 20]]
    pairs_b = [[30, 40], [30, 50]]
    await store.put(dimension_id=1, pairs=pairs_a, read_version=_VERSION_0, write_version=_VERSION_1)
    await store.put(dimension_id=2, pairs=pairs_b, read_version=_VERSION_1, write_version=_VERSION_2)
    result_a = await store.get_pairs(dimension_id=1, read_version=_VERSION_2)
    result_b = await store.get_pairs(dimension_id=2, read_version=_VERSION_2)
    assert result_a == pairs_a
    assert result_b == pairs_b

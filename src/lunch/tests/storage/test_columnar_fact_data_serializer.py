"""Tests for ColumnarFactDataSerializer partition index."""

from pathlib import Path

from src.lunch.mvcc.version import Version
from src.lunch.storage.persistence.stringio_columnar_fact_data_persistor import (
    StringIOColumnarFactDataPersistor,
)
from src.lunch.storage.serialization.columnar_fact_data_serializer import (
    ColumnarFactDataSerializer,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_DIR = Path("/fake/root")

_V0 = Version(
    version=0,
    model_version=0,
    reference_data_version=0,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)
_V1 = Version(
    version=1,
    model_version=1,
    reference_data_version=0,
    cube_data_version=1,
    operations_version=0,
    website_version=0,
)
_V2 = Version(
    version=2,
    model_version=1,
    reference_data_version=0,
    cube_data_version=2,
    operations_version=0,
    website_version=0,
)


def _make_serializer():
    persistor = StringIOColumnarFactDataPersistor(directory=_DIR)
    return ColumnarFactDataSerializer(persistor=persistor)


# ---------------------------------------------------------------------------
# get_partition_index
# ---------------------------------------------------------------------------


async def test_get_partition_index_returns_empty_dict_when_no_file_exists():
    """get_partition_index returns {} when no partition index file has been written."""
    serializer = _make_serializer()
    result = await serializer.get_partition_index(version=_V1)
    assert result == {}


async def test_get_partition_index_returns_empty_dict_for_version_zero():
    """get_partition_index returns {} for version with cube_data_version=0."""
    serializer = _make_serializer()
    result = await serializer.get_partition_index(version=_V0)
    assert result == {}


async def test_get_partition_index_returns_stored_index():
    """get_partition_index returns the index previously written by put_partition_index."""
    serializer = _make_serializer()
    index = {1: 1, 2: 1}

    await serializer.put_partition_index(index_=index, version=_V1)
    result = await serializer.get_partition_index(version=_V1)

    assert result == index


async def test_get_partition_index_is_isolated_by_version():
    """Partition indexes at different versions are stored independently."""
    serializer = _make_serializer()
    index_v1 = {1: 1}
    index_v2 = {1: 2, 2: 2}

    await serializer.put_partition_index(index_=index_v1, version=_V1)
    await serializer.put_partition_index(index_=index_v2, version=_V2)

    assert await serializer.get_partition_index(version=_V1) == index_v1
    assert await serializer.get_partition_index(version=_V2) == index_v2


# ---------------------------------------------------------------------------
# put_partition_index
# ---------------------------------------------------------------------------


async def test_put_partition_index_overwrites_previous_index():
    """put_partition_index replaces the existing index at the same version."""
    serializer = _make_serializer()

    await serializer.put_partition_index(index_={1: 1}, version=_V1)
    await serializer.put_partition_index(index_={2: 1}, version=_V1)

    result = await serializer.get_partition_index(version=_V1)
    assert result == {2: 1}


async def test_put_partition_index_empty_dict():
    """put_partition_index handles an empty dict correctly."""
    serializer = _make_serializer()

    await serializer.put_partition_index(index_={}, version=_V1)
    result = await serializer.get_partition_index(version=_V1)

    assert result == {}

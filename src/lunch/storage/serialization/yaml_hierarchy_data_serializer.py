import yaml

from src.lunch.mvcc.version import Version
from src.lunch.storage.persistence.hierarchy_data_persistor import HierarchyDataPersistor
from src.lunch.storage.serialization.hierarchy_data_serializer import HierarchyDataSerializer


class YamlHierarchyDataSerializer(HierarchyDataSerializer):
    """YAML-backed serializer for hierarchy (parent-child relationship) data."""

    def __init__(self, persistor: HierarchyDataPersistor):
        self._persistor = persistor

    async def get_pairs(self, dimension_id: int, reference_data_version: int) -> list:
        return await _get_pairs(
            dimension_id=dimension_id,
            reference_data_version=reference_data_version,
            persistor=self._persistor,
        )

    async def put_pairs(self, dimension_id: int, pairs: list, version: Version) -> None:
        return await _put_pairs(
            dimension_id=dimension_id,
            pairs=pairs,
            version=version,
            persistor=self._persistor,
        )

    async def get_version_index(self, version: Version) -> dict:
        return await _get_version_index(version=version, persistor=self._persistor)

    async def put_version_index(self, index_: dict, version: Version) -> None:
        return await _put_version_index(index_=index_, version=version, persistor=self._persistor)


async def _get_pairs(
    dimension_id: int,
    reference_data_version: int,
    persistor: HierarchyDataPersistor,
) -> list:
    try:
        with persistor.open_pairs_file_read(dimension_id=dimension_id, version=reference_data_version) as stream:
            data = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError((dimension_id, reference_data_version))
    return data or []


async def _put_pairs(
    dimension_id: int,
    pairs: list,
    version: Version,
    persistor: HierarchyDataPersistor,
) -> None:
    with persistor.open_pairs_file_write(dimension_id=dimension_id, version=version.reference_data_version) as stream:
        yaml.safe_dump(pairs, stream)


async def _get_version_index(version: Version, persistor: HierarchyDataPersistor) -> dict:
    if not version.reference_data_version:
        return {}
    try:
        with persistor.open_version_index_file_read(version=version.reference_data_version) as stream:
            index_ = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return index_ or {}


async def _put_version_index(index_: dict, version: Version, persistor: HierarchyDataPersistor) -> None:
    with persistor.open_version_index_file_write(version=version.reference_data_version) as stream:
        yaml.safe_dump(index_, stream)

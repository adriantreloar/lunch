from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.hierarchy_data_cache import HierarchyDataCache


class NullHierarchyDataCache(HierarchyDataCache):
    """Null cache for hierarchy data — always misses on reads, discards writes."""

    async def abort_write(self, version: Version):
        pass

    async def get_version_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_version_index(self, index_: dict, version: Version) -> None:
        pass

    async def get_pairs(self, dimension_id: int, reference_data_version: int) -> list:
        raise KeyError((dimension_id, reference_data_version))

    async def put_pairs(self, dimension_id: int, reference_data_version: int, pairs: list) -> None:
        pass

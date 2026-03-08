from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.cache import Cache


class HierarchyDataCache(Cache):
    """Abstract base cache for hierarchy (parent-child relationship) data."""

    async def abort_write(self, version: Version):
        raise NotImplementedError("Abstract")

    async def get_version_index(self, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_version_index(self, index_: dict, version: Version) -> None:
        raise NotImplementedError("Abstract")

    async def get_pairs(self, dimension_id: int, reference_data_version: int) -> list:
        raise NotImplementedError("Abstract")

    async def put_pairs(self, dimension_id: int, reference_data_version: int, pairs: list) -> None:
        raise NotImplementedError("Abstract")

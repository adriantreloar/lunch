from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.cache import Cache


class HierarchyDataCache(Cache):
    """Abstract base cache for hierarchy (parent-child relationship) data."""

    async def abort_write(self, version: Version):
        """
        Clear out half-written data in the cache for an aborted write version.

        :param version: Write version that has been aborted
        """
        raise NotImplementedError("Abstract")

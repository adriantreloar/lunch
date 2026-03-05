from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.hierarchy_data_cache import HierarchyDataCache


class NullHierarchyDataCache(HierarchyDataCache):
    """
    Placeholder null cache for hierarchy data.
    All methods raise NotImplementedError — hierarchy storage is not yet implemented.
    """

    async def abort_write(self, version: Version):
        raise NotImplementedError

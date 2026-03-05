from src.lunch.storage.cache.hierarchy_data_cache import HierarchyDataCache
from src.lunch.storage.serialization.hierarchy_data_serializer import HierarchyDataSerializer
from src.lunch.storage.store import Store


class HierarchyDataStore(Store):
    """
    Placeholder store for hierarchy (parent-child relationship) data.

    A hierarchy defines a parent-child ordering of dimension members — for example
    a Time dimension might expose a Year → Quarter → Month hierarchy.

    Follows the same Store → Serializer → Persistor three-layer pattern as
    DimensionDataStore. All methods raise NotImplementedError; no hierarchy
    operations are implemented yet.
    """

    def __init__(
        self,
        serializer: HierarchyDataSerializer,
        cache: HierarchyDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

    def storage_instructions(self, version):
        raise NotImplementedError

    async def put(self, *args, **kwargs):
        raise NotImplementedError

    async def get_columns(self, *args, **kwargs):
        raise NotImplementedError

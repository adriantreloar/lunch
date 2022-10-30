from lunch.storage.cache.dimension_data_cache import DimensionDataCache
from lunch.storage.serialization.dimension_data_serializer import (
    DimensionDataSerializer,
)
from lunch.storage.store import Store


class DimensionDataStore(Store):
    def __init__(
        self,
        serializer: DimensionDataSerializer,
        cache: DimensionDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

    @property
    def storage_instructions(self):
        return dict()

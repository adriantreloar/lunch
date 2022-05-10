from lunch.storage.store import Store
from lunch.storage.cache.reference_data_cache import ReferenceDataCache
from lunch.storage.serialization.reference_data_serializer import ReferenceDataSerializer


class ReferenceDataStore(Store):

    def __init__(
        self,
        serializer: ReferenceDataSerializer,
        cache: ReferenceDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

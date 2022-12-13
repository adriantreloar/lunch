from src.lunch.storage.cache.reference_data_cache import ReferenceDataCache
from src.lunch.storage.serialization.reference_data_serializer import (
    ReferenceDataSerializer,
)
from src.lunch.storage.store import Store


class ReferenceDataStore(Store):
    """
    JAT 20221031
    Not sure what this was for - my guess is cross references for input dimensions
    """

    def __init__(
        self,
        serializer: ReferenceDataSerializer,
        cache: ReferenceDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

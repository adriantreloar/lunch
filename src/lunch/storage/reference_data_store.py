from src.lunch.storage.cache.reference_data_cache import ReferenceDataCache
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.hierarchy_data_store import HierarchyDataStore
from src.lunch.storage.serialization.reference_data_serializer import (
    ReferenceDataSerializer,
)
from src.lunch.storage.store import Store


class ReferenceDataStore(Store):
    """
    Top-level store providing a unified, versioned interface to all reference data.

    Reference data is divided into two sub-stores:

    - DimensionDataStore: manages dimension member (attribute) data — the actual rows
      belonging to each dimension at each version.
    - HierarchyDataStore: manages hierarchy (parent-child relationship) data — planned
      but not yet implemented; all methods raise NotImplementedError.

    ReferenceDataStore itself is responsible for:

    - Owning and exposing the sub-stores via properties (dimension_data_store,
      hierarchy_data_store).
    - Managing the overall reference data version index: a version-level record of which
      sub-store (dimension data, hierarchy data, or both) has changed in a given version.
      This allows change detection across all reference data without inspecting each
      sub-store individually.
    - Delegating dimension data operations to DimensionDataStore.
    - Delegating hierarchy operations to HierarchyDataStore (once implemented).

    The serializer and cache held directly on this class are used exclusively for
    index management, not for data storage (which is delegated to the sub-stores).
    """

    def __init__(
        self,
        dimension_data_store: DimensionDataStore,
        hierarchy_data_store: HierarchyDataStore,
        serializer: ReferenceDataSerializer,
        cache: ReferenceDataCache,
    ):
        self._dimension_data_store = dimension_data_store
        self._hierarchy_data_store = hierarchy_data_store
        self._serializer = serializer
        self._cache = cache

    @property
    def dimension_data_store(self) -> DimensionDataStore:
        return self._dimension_data_store

    @property
    def hierarchy_data_store(self) -> HierarchyDataStore:
        return self._hierarchy_data_store

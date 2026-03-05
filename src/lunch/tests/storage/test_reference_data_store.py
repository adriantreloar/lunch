from mock import Mock

from src.lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.hierarchy_data_store import HierarchyDataStore
from src.lunch.storage.reference_data_store import ReferenceDataStore
from src.lunch.storage.serialization.yaml_reference_data_serializer import YamlReferenceDataSerializer


def _make_store():
    dim_store = Mock(DimensionDataStore)
    hier_store = Mock(HierarchyDataStore)
    serializer = Mock(YamlReferenceDataSerializer)
    cache = NullReferenceDataCache()
    store = ReferenceDataStore(
        dimension_data_store=dim_store,
        hierarchy_data_store=hier_store,
        serializer=serializer,
        cache=cache,
    )
    return store, dim_store, hier_store


def test_dimension_data_store_property_returns_injected_store():
    store, dim_store, _ = _make_store()
    assert store.dimension_data_store is dim_store


def test_hierarchy_data_store_property_returns_injected_store():
    store, _, hier_store = _make_store()
    assert store.hierarchy_data_store is hier_store


def test_dimension_and_hierarchy_stores_are_independent():
    store, dim_store, hier_store = _make_store()
    assert store.dimension_data_store is not store.hierarchy_data_store

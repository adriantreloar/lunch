from src.lunch.storage.persistence.hierarchy_data_persistor import HierarchyDataPersistor


class NullHierarchyDataPersistor(HierarchyDataPersistor):
    """
    Placeholder null persistor for hierarchy data.
    All methods raise NotImplementedError — hierarchy storage is not yet implemented.
    """
    pass

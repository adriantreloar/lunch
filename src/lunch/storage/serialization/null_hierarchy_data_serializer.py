from src.lunch.storage.serialization.hierarchy_data_serializer import HierarchyDataSerializer


class NullHierarchyDataSerializer(HierarchyDataSerializer):
    """
    Placeholder null serializer for hierarchy data.
    All methods raise NotImplementedError — hierarchy storage is not yet implemented.
    """
    pass

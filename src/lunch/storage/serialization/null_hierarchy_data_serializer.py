from src.lunch.mvcc.version import Version
from src.lunch.storage.serialization.hierarchy_data_serializer import HierarchyDataSerializer


class NullHierarchyDataSerializer(HierarchyDataSerializer):
    """Null serializer for hierarchy data — always misses on reads, discards writes."""

    async def get_pairs(self, dimension_id: int, reference_data_version: int) -> list:
        raise KeyError((dimension_id, reference_data_version))

    async def put_pairs(self, dimension_id: int, pairs: list, version: Version) -> None:
        pass

    async def get_version_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_version_index(self, index_: dict, version: Version) -> None:
        pass

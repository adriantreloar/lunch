from src.lunch.mvcc.version import Version
from src.lunch.storage.serialization.serializer import Serializer


class HierarchyDataSerializer(Serializer):
    """Abstract base serializer for hierarchy (parent-child relationship) data.

    Pairs are stored as a list of [parent_member_id, child_member_id] entries.
    The version index maps dimension_id → reference_data_version.
    """

    async def get_pairs(self, dimension_id: int, reference_data_version: int) -> list:
        raise NotImplementedError("Abstract")

    async def put_pairs(self, dimension_id: int, pairs: list, version: Version) -> None:
        raise NotImplementedError("Abstract")

    async def get_version_index(self, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_version_index(self, index_: dict, version: Version) -> None:
        raise NotImplementedError("Abstract")

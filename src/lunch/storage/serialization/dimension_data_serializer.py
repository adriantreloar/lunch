from typing import Iterable, Mapping

from numpy.typing import DTypeLike

from src.lunch.mvcc.version import Version
from src.lunch.storage.serialization.serializer import Serializer


class DimensionDataSerializer(Serializer):
    pass

    async def get_columns(
        self,
        reference_data_version: int,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
    ) -> Mapping[int, Iterable]:
        raise NotImplementedError("Abstract")

    async def put_columns(
        self,
        version: Version,
        dimension_id: int,
        columns: Mapping[int, Iterable],
    ) -> None:
        raise NotImplementedError("Abstract")

    async def get_version_index(self, version: Version) -> dict[int, int]:
        raise NotImplementedError("Abstract")

    async def put_version_index(self, index_: dict[int, int], version: Version):
        raise NotImplementedError("Abstract")

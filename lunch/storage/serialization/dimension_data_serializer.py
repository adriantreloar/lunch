from typing import AsyncIterable, Iterable, Mapping

from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.serialization.serializer import Serializer


class DimensionDataSerializer(Serializer):
    pass

    async def get_columns(
        self,
        reference_data_version: int,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
    ) -> Mapping[int, Iterable]:
        raise NotImplementedError("Abstract")

    async def get_version_index(self, version: Version) -> dict[int, int]:
        raise NotImplementedError("Abstract")

    async def put_version_index(self, index_: dict[int, int], version: Version):
        raise NotImplementedError("Abstract")

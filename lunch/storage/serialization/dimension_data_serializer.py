from typing import AsyncIterable, Iterable, Mapping

from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.serialization.serializer import Serializer


class DimensionDataSerializer(Serializer):
    pass

    async def get_columns(
        self,
        read_version: Version,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
    ) -> Mapping[int, AsyncIterable]:
        raise NotImplementedError("Abstract")

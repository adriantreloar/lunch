from typing import Any, Iterable

from numpy import dtype

from lunch.mvcc.version import Version
from lunch.storage.cache.dimension_data_cache import DimensionDataCache
from lunch.storage.serialization.dimension_data_serializer import (
    DimensionDataSerializer,
)
from lunch.storage.store import Store


class DimensionDataStore(Store):
    def __init__(
        self,
        serializer: DimensionDataSerializer,
        cache: DimensionDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

    def storage_instructions(self, version: Version):
        return dict()

    async def put(
        self,
        dimension_id: int,
        columnar_data: dict[int, Iterable],
        write_version: Version,
    ) -> None:
        raise NotImplementedError("TODO")

    async def get(
        self, read_version: Version, dimension_id: int, filter: Any | None
    ) -> tuple[dict[int, Iterable], dict[int, dtype]]:
        """

        :param read_version:
        :param dimension_id:
        :param filter: TDOO not really sure what shape this will be. mongo style filter?
        :return:
        """
        raise NotImplementedError("TODO")

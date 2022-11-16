from typing import Any, Iterable, Mapping

from numpy import dtype
from numpy.typing import DTypeLike

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

    async def get_columns(
        self,
        read_version: Version,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
        filter: Any | None,
    ) -> Mapping[int, Iterable]:
        """
        Get columnar data

        :param read_version:
        :param dimension_id:
        :param column_types: attribute_ids vs type for the column, so it can be read from the file
        :param filter: TODO not really sure what shape this will be. mongo style filter?
        :return: dict - column integer ids to iterables, dict column integer ids to types
        """

        version_index = await self._serializer.get_version_index(version=read_version)

        reference_data_version = version_index[dimension_id]

        try:
            column_data = await self._cache.get_columns(
                dimension_id=dimension_id, reference_data_version=reference_data_version
            )
        except KeyError:
            column_data = await self._serializer.get_columns(
                dimension_id=dimension_id,
                reference_data_version=reference_data_version,
                column_types=column_types,
            )
            await self._cache.put_columns(
                dimension_id=dimension_id,
                reference_data_version=reference_data_version,
                column_data=column_data,
                column_types=column_types,
            )

        return column_data

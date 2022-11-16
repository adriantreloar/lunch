from typing import Any, Iterable, Mapping

from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.cache.dimension_data_cache import DimensionDataCache
from lunch.storage.serialization.dimension_data_serializer import (
    DimensionDataSerializer,
)
from lunch.storage.store import Store
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)


class DimensionDataStore(Store):
    def __init__(
        self,
        serializer: DimensionDataSerializer,
        cache: DimensionDataCache,
    ):
        self._serializer = serializer
        self._cache = cache
        self._dimension_index_transformer = DimensionIndexTransformer()

    def storage_instructions(self, version: Version):
        return dict()

    async def put(
        self,
        dimension_id: int,
        columnar_data: dict[int, Iterable],
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _put(
            dimension_id=dimension_id,
            columnar_data=columnar_data,
            read_version=read_version,
            dimension_index_transformer=self._dimension_index_transformer,
            write_version=write_version,
            serializer=self._serializer,
            cache=self._cache,
        )

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
                version=read_version,
                columns=column_data,
            )

        return column_data


async def _put(
    dimension_id: int,
    columnar_data: dict[int, Iterable],
    read_version: Version,
    write_version: Version,
    dimension_index_transformer: DimensionIndexTransformer,
    serializer: DimensionDataSerializer,
    cache: DimensionDataCache,
) -> None:

    # We want to update the version index for the write (should be in cache)
    # If it doesn't exist, start with the read version index
    try:
        dimensions_version_index_write = await _get_dimension_version_index(
            version=write_version, serializer=serializer, cache=cache
        )
    except KeyError:
        dimensions_version_index_write = await _get_dimension_version_index(
            version=read_version, serializer=serializer, cache=cache
        )

    # All the changed dimensions will be in dimensions_with_ids now
    # All of these have a version of the write-version
    dimensions_version_index_write = (
        dimension_index_transformer.update_dimension_version_index(
            index_=dimensions_version_index_write,
            write_version=write_version,
            changed_ids=[dimension_id],
        )
    )

    await _put_dimension_version_index(
        index_=dimensions_version_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_columns(
        dimension_id=dimension_id,
        columns=columnar_data,
        version=write_version,
    )
    await cache.put_columns(
        dimension_id=dimension_id,
        columns=columnar_data,
        version=write_version,
    )


async def _get_dimension_version_index(
    version: Version,
    serializer: DimensionDataSerializer,
    cache: DimensionDataCache,
) -> dict[int, int]:
    raise NotImplementedError("TODO")


async def _put_dimension_version_index(
    index_: dict,
    version: Version,
    serializer: DimensionDataSerializer,
    cache: DimensionDataCache,
) -> None:
    raise NotImplementedError("TODO")

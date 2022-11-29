from typing import Any, Iterable, Mapping

from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.cache.fact_data_cache import FactDataCache
from lunch.storage.serialization.fact_data_serializer import (
    FactDataSerializer,
)
from lunch.storage.store import Store
from lunch.storage.transformers.fact_data_version_index_transformer import (
    FactDataVersionIndexTransformer,
)
from lunch.storage.transformers.fact_data_partition_index_transformer import (
    FactDataPartitionIndexTransformer,
)


class FactDataStore(Store):
    def __init__(
        self,
        serializer: FactDataSerializer,
        cache: FactDataCache,
    ):
        self._serializer = serializer
        self._cache = cache
        self._fact_data_version_index_transformer = FactDataVersionIndexTransformer()
        self._fact_data_partition_index_transformer = FactDataPartitionIndexTransformer()

    def storage_instructions(self, version: Version):
        return dict()

    async def put(
        self,
        fact_id: int,
        columnar_data: dict[int, Iterable],
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _put(
            fact_id=fact_id,
            columnar_data=columnar_data,
            read_version=read_version,
            fact_data_version_index_transformer=self._fact_data_version_index_transformer,
            fact_data_partition_index_transformer=self._fact_data_partition_index_transformer,
            write_version=write_version,
            serializer=self._serializer,
            cache=self._cache,
        )

    async def get_columns(
        self,
        read_version: Version,
        fact_id: int,
        column_types: Mapping[int, DTypeLike],
        filter: Any | None,
    ) -> Mapping[int, Iterable]:
        """
        Get columnar data

        :param read_version:
        :param fact_id:
        :param column_types: attribute_ids vs type for the column, so it can be read from the file
        :param filter: TODO not really sure what shape this will be. mongo style filter?
        :return: dict - column integer ids to iterables, dict column integer ids to types
        """

        version_index = await _get_version_index(serializer=self._serializer, cache=self._cache, version=read_version)

        reference_data_version = version_index[fact_id]

        try:
            column_data = await self._cache.get_columns(
                fact_id=fact_id, reference_data_version=reference_data_version
            )
        except KeyError:
            column_data = await self._serializer.get_columns(
                fact_id=fact_id,
                reference_data_version=reference_data_version,
                column_types=column_types,
            )
            await self._cache.put_columns(
                fact_id=fact_id,
                version=read_version,
                columns=column_data,
            )

        return column_data


async def _put(
    fact_id: int,
    columnar_data: dict[int, Iterable],
    read_version: Version,
    write_version: Version,
    fact_data_version_index_transformer: FactDataVersionIndexTransformer,
    fact_data_partition_index_transformer: FactDataPartitionIndexTransformer,
    serializer: FactDataSerializer,
    cache: FactDataCache,
) -> None:

    # We want to update the version index for the write (should be in cache)
    # If it doesn't exist, start with the read version index
    try:
        facts_version_index_write = await _get_version_index(
            version=write_version, serializer=serializer, cache=cache
        )
    except KeyError:
        facts_version_index_write = await _get_version_index(
            version=read_version, serializer=serializer, cache=cache
        )

    # All the changed facts will be in facts_with_ids now
    # All of these have a version of the write-version
    facts_version_index_write = (
        fact_data_index_transformer.update_fact_version_index(
            index_=facts_version_index_write,
            write_version=write_version,
            changed_ids=[fact_id],
        )
    )

    await _put_version_index(
        index_=facts_version_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )

    # We want to update the partition index for the write (should be in cache)
    # If it doesn't exist, start with the read version partition index
    try:
        facts_partition_index_write = await _get_partition_index(
            version=write_version, serializer=serializer, cache=cache
        )
    except KeyError:
        facts_partition_index_write = await _get_partition_index(
            version=read_version, serializer=serializer, cache=cache
        )

    # All the changed facts will be in facts_with_ids now
    # All of these have a version of the write-version
    facts_partition_index_write = (
        fact_data_partition_index_transformer.update_fact_partition_index(
            index_=facts_partition_index_write,
            write_version=write_version,
            changed_ids=[ TODO get changed IDS],
        )
    )

    await _put_version_index(
        index_=facts_version_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_columns(
        fact_id=fact_id,
        columns=columnar_data,
        version=write_version,
    )
    await cache.put_columns(
        fact_id=fact_id,
        columns=columnar_data,
        version=write_version,
    )


async def _get_version_index(
    version: Version,
    serializer: FactDataSerializer,
    cache: FactDataCache,
) -> dict[int, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_version_index(version=version)
    except KeyError:
        return await serializer.get_version_index(version=version)

async def _put_version_index(
    index_: dict,
    version: Version,
    serializer: FactDataSerializer,
    cache: FactDataCache,
) -> None:
    await serializer.put_version_index(index_=index_, version=version)
    await cache.put_version_index(index_=index_, version=version)


async def _get_partition_index(
    version: Version,
    serializer: FactDataSerializer,
    cache: FactDataCache,
) -> dict[int, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_partition_index(version=version)
    except KeyError:
        return await serializer.get_partition_index(version=version)

async def _put_partition_index(
    index_: dict,
    version: Version,
    serializer: FactDataSerializer,
    cache: FactDataCache,
) -> None:
    await serializer.put_partition_index(index_=index_, version=version)
    await cache.put_partition_index(index_=index_, version=version)
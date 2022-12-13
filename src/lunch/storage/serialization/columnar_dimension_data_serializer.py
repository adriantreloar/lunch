import asyncio
from typing import Any, AsyncIterator, Iterable, Iterator, Mapping

# TODO, don't use YAML, use binary index file, maybe move index file creation off separately
import yaml
from numpy.typing import DTypeLike

from src.lunch.mvcc.version import Version
from src.lunch.storage.persistence.local_file_columnar_dimension_data_persistor import (
    LocalFileColumnarDimensionDataPersistor,
)
from src.lunch.storage.serialization.dimension_data_serializer import (
    DimensionDataSerializer,
)


class ColumnarDimensionDataSerializer(DimensionDataSerializer):
    """ """

    def __init__(self, persistor: LocalFileColumnarDimensionDataPersistor):
        self._persistor = persistor

    async def get_version_index(self, version: Version) -> dict[int, int]:
        return await _get_version_index(version=version, persistor=self._persistor)

    async def put_version_index(self, index_: dict[int, int], version: Version):
        return await _put_version_index(
            index_=index_, version=version, persistor=self._persistor
        )

    def get_attribute_data(
        self, dimension_id: int, attribute_id: int, reference_data_version: int
    ) -> Iterable[str]:
        for i in _get_attribute_data(
            dimension_id, attribute_id, reference_data_version, self._persistor
        ):
            yield i

    async def put_index_data(
        self, dimension_id: int, version: Version, index_iterator
    ) -> None:
        await _put_index_data(
            dimension_id=dimension_id,
            version=version,
            index_iterator=index_iterator,
            persistor=self._persistor,
        )


    async def put_attribute_data(
        self,
        dimension_id: int,
        attribute_id: int,
        version: Version,
        attribute_data: Iterable[Any],
    ) -> None:
        await _put_attribute_data(
            dimension_id=dimension_id,
            version=version,
            attribute_id=attribute_id,
            attribute_data=attribute_data,
            persistor=self._persistor,
        )

    async def get_columns(
        self,
        reference_data_version: int,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
    ) -> Mapping[int, Iterable]:
        return await _get_columns(
            reference_data_version=reference_data_version,
            dimension_id=dimension_id,
            column_types=column_types,
            persistor=self._persistor,
        )

    async def put_columns(
        self,
        version: Version,
        dimension_id: int,
        columns: Mapping[int, Iterable],
    ) -> None:
        return await _put_columns(
            version=version,
            dimension_id=dimension_id,
            columns=columns,
            persistor=self._persistor,
        )


async def _get_version_index(
    version: Version, persistor: LocalFileColumnarDimensionDataPersistor
) -> dict[int, int]:
    if not version.reference_data_version:
        return {}

    try:
        with persistor.open_version_index_file_read(
            version=version.reference_data_version,
        ) as stream:
            version_index = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)

    return version_index


async def _put_version_index(
    index_: dict, version: Version, persistor: LocalFileColumnarDimensionDataPersistor
):
    with persistor.open_version_index_file_write(
        version=version.reference_data_version
    ) as stream:
        yaml.safe_dump(index_, stream)


async def _get_version_data(
    dimension_id: int,
    version: Version,
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> AsyncIterator[int]:
    yield 0


async def _put_index_data(
    dimension_id: int,
    version: Version,
    index_iterator: Iterator[int],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> None:
    pass


def _get_attribute_data(
    dimension_id: int,
    attribute_id: int,
    reference_data_version: int,
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> Iterable[str]:
    with persistor.open_attribute_file_read(
        dimension_id=dimension_id,
        attribute_id=attribute_id,
        version=reference_data_version,
    ) as f:
        for line in f:
            yield line


async def _get_columns(
    reference_data_version: int,
    dimension_id: int,
    column_types: Mapping[int, DTypeLike],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> Mapping[int, Iterable]:
    return {
        attribute_id: _get_attribute_data(
            dimension_id=dimension_id,
            attribute_id=attribute_id,
            reference_data_version=reference_data_version,
            persistor=persistor,
        )
        for attribute_id, attribute_type in column_types.items()
    }


async def _put_columns(
    version: Version,
    dimension_id: int,
    columns: Mapping[int, Iterable],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> None:
    coros = [
        _put_attribute_data(
            dimension_id=dimension_id,
            attribute_id=attribute_id,
            attribute_data=attribute_data,
            version=version,
            persistor=persistor,
        )
        for attribute_id, attribute_data in columns.items()
    ]

    await asyncio.gather(*coros)


# TODO - if the dimension is flagged as newlines allowed, then separate with glagolytic, or whatever has been decided
#  or have a version of this code that has two files - one for the strings, and one for start positions of the strings
#  a further extension is to add bitmap indices to another file, for nullability
async def _put_attribute_data(
    dimension_id: int,
    attribute_id: int,
    version: Version,
    attribute_data: Iterable[Any],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> None:
    with persistor.open_attribute_file_write(
        dimension_id=dimension_id,
        attribute_id=attribute_id,
        version=version.reference_data_version,
    ) as f:
        for attribute in attribute_data:
            f.write(str(attribute))
            f.write("\n")

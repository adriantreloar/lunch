from typing import Any, AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping

from numpy import dtype
from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.persistence.local_file_columnar_dimension_data_persistor import (
    LocalFileColumnarDimensionDataPersistor,
)
from lunch.storage.serialization.dimension_data_serializer import (
    DimensionDataSerializer,
)


class ColumnarDimensionDataSerializer(DimensionDataSerializer):
    """ """

    def __init__(self, persistor: LocalFileColumnarDimensionDataPersistor):
        self._persistor = persistor

    async def get_index_data(
        self, dimension_id: int, version: Version
    ) -> AsyncIterator[int]:
        async for i in _get_index_data(dimension_id, version, self._persistor):
            yield i

    # TODO: convert type
    def get_attribute_data(
        self, dimension_id: int, attribute_id: int, version: Version
    ) -> Iterable[str]:
        for i in _get_attribute_data(
            dimension_id, attribute_id, version, self._persistor
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

    # TODO: convert type, or have different methods for different types - e.g. string, string with newlines, etc.
    async def put_attribute_data(
        self,
        dimension_id: int,
        attribute_id: int,
        version: Version,
        attribute_iterator: Iterator[Any],
    ) -> None:
        await _put_attribute_data(
            dimension_id=dimension_id,
            version=version,
            attribute_id=attribute_id,
            attribute_iterator=attribute_iterator,
            persistor=self._persistor,
        )

    async def get_columns(
        self,
        read_version: Version,
        dimension_id: int,
        column_types: Mapping[int, DTypeLike],
    ) -> Mapping[int, Iterable]:
        return await _get_columns(
            read_version=read_version,
            dimension_id=dimension_id,
            column_types=column_types,
            persistor=self._persistor,
        )


async def _get_index_data(
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
    version: Version,
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> Iterable[str]:
    # TODO - this is not an async iterator is it - we need an async file read
    with persistor.open_attribute_file_read(
        dimension_id=dimension_id,
        attribute_id=attribute_id,
        version=version.reference_data_version,
    ) as f:
        for line in f:
            yield line


async def _get_columns(
    read_version: Version,
    dimension_id: int,
    column_types: Mapping[int, DTypeLike],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> Mapping[int, Iterable]:
    return {
        attribute_id: _get_attribute_data(
            dimension_id=dimension_id,
            attribute_id=attribute_id,
            version=read_version,
            persistor=persistor,
        )
        for attribute_id, attribute_type in column_types.items()
    }


# TODO - if the dimension is flagged as newlines allowed, then separate with glagolytic, or whatever has been decided
#  or have a version of this code that has two files - one for the strings, and one for start positions of the strings
#  a further extension is to add bitmap indices to another file, for nullability
async def _put_attribute_data(
    dimension_id: int,
    attribute_id: int,
    version: Version,
    attribute_iterator: Iterator[Any],
    persistor: LocalFileColumnarDimensionDataPersistor,
) -> None:
    with persistor.open_attribute_file_write(
        dimension_id=dimension_id,
        attribute_id=attribute_id,
        version=version.reference_data_version,
    ) as f:
        for attribute in attribute_iterator:
            f.write(str(attribute))
            f.write("\n")

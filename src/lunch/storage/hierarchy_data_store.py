from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.hierarchy_data_cache import HierarchyDataCache
from src.lunch.storage.serialization.hierarchy_data_serializer import HierarchyDataSerializer
from src.lunch.storage.store import Store


class HierarchyDataStore(Store):
    """Store for hierarchy (parent-child relationship) data.

    A hierarchy defines a parent-child ordering of dimension members — for
    example a Time dimension might expose a Year → Quarter → Month hierarchy.

    Follows the same Store → Serializer → Persistor three-layer pattern as
    ``DimensionDataStore``.  Data is stored as a list of
    ``[parent_member_id, child_member_id]`` pairs keyed by ``dimension_id``.
    The version index maps ``dimension_id`` to the ``reference_data_version``
    at which that dimension's hierarchy was last written.
    """

    def __init__(
        self,
        serializer: HierarchyDataSerializer,
        cache: HierarchyDataCache,
    ):
        self._serializer = serializer
        self._cache = cache

    async def put(
        self,
        dimension_id: int,
        pairs: list,
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _put(
            dimension_id=dimension_id,
            pairs=pairs,
            read_version=read_version,
            write_version=write_version,
            serializer=self._serializer,
            cache=self._cache,
        )

    async def get_pairs(
        self,
        dimension_id: int,
        read_version: Version,
    ) -> list:
        return await _get_pairs(
            dimension_id=dimension_id,
            read_version=read_version,
            serializer=self._serializer,
            cache=self._cache,
        )


async def _put(
    dimension_id: int,
    pairs: list,
    read_version: Version,
    write_version: Version,
    serializer: HierarchyDataSerializer,
    cache: HierarchyDataCache,
) -> None:
    try:
        version_index = await _get_version_index(version=write_version, serializer=serializer, cache=cache)
    except KeyError:
        try:
            version_index = await _get_version_index(version=read_version, serializer=serializer, cache=cache)
        except KeyError:
            version_index = {}

    version_index[dimension_id] = write_version.reference_data_version

    await _put_version_index(index_=version_index, version=write_version, serializer=serializer, cache=cache)
    await serializer.put_pairs(dimension_id=dimension_id, pairs=pairs, version=write_version)
    await cache.put_pairs(
        dimension_id=dimension_id,
        reference_data_version=write_version.reference_data_version,
        pairs=pairs,
    )


async def _get_pairs(
    dimension_id: int,
    read_version: Version,
    serializer: HierarchyDataSerializer,
    cache: HierarchyDataCache,
) -> list:
    version_index = await _get_version_index(version=read_version, serializer=serializer, cache=cache)
    reference_data_version = version_index[dimension_id]

    try:
        return await cache.get_pairs(
            dimension_id=dimension_id,
            reference_data_version=reference_data_version,
        )
    except KeyError:
        pairs = await serializer.get_pairs(
            dimension_id=dimension_id,
            reference_data_version=reference_data_version,
        )
        await cache.put_pairs(
            dimension_id=dimension_id,
            reference_data_version=reference_data_version,
            pairs=pairs,
        )
        return pairs


async def _get_version_index(
    version: Version,
    serializer: HierarchyDataSerializer,
    cache: HierarchyDataCache,
) -> dict:
    if not version.reference_data_version:
        return {}
    try:
        return await cache.get_version_index(version=version)
    except KeyError:
        return await serializer.get_version_index(version=version)


async def _put_version_index(
    index_: dict,
    version: Version,
    serializer: HierarchyDataSerializer,
    cache: HierarchyDataCache,
) -> None:
    await serializer.put_version_index(index_=index_, version=version)
    await cache.put_version_index(index_=index_, version=version)

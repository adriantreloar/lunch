from lunch.mvcc.version import Version
from lunch.storage.cache.version_cache import VersionCache
from lunch.storage.serialization.version_serializer import VersionSerializer
from lunch.storage.store import Store


class VersionStore(Store):
    """Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """

    pass

    def __init__(self, serializer: VersionSerializer, cache: VersionCache):
        self._serializer = serializer
        self._cache = cache

    async def begin_read(
        self,
    ) -> Version:
        """
        Start a read, incrementing the number of readers for the returned version
        :return:
        """
        return await _begin_read(self._serializer, self._cache)

    async def end_read(self, version: Version) -> Version:
        """
        End a read, decrementing the number of readers for the version
        :return: The latest read version, so we can tell if things have moved on
        """
        return await _end_read(version, self._serializer, self._cache)

    async def begin_write_model(self, read_version: Version) -> Version:
        """

        :param read_version:
        :return:
        """
        return await _begin_write_model(read_version, self._serializer, self._cache)

    async def begin_write_reference_data(self, read_version: Version) -> Version:
        """

        :param read_version:
        :return:
        """
        return await _begin_write_reference_data(
            read_version, self._serializer, self._cache
        )

    async def abort(self, version: Version) -> Version:
        """

        :param version:
        :return: The latest version
        """
        return await _commit(
            version=version, serializer=self._serializer, cache=self._cache
        )

    async def commit(self, version: Version) -> Version:
        """

        :param version:
        :return: The latest version
        """
        return await _commit(
            version=version, serializer=self._serializer, cache=self._cache
        )


async def _begin_read(serializer: VersionSerializer, cache: VersionCache) -> Version:
    """

    :param serializer:
    :param cache:
    :return: Current read version
    """

    current_version = await serializer.begin_read()
    # By tracking readers locally, we can vacuum the cache more efficiently
    await cache.increment_readers(current_version)
    return current_version


async def _end_read(
    version: Version, serializer: VersionSerializer, cache: VersionCache
) -> Version:
    """

    :param version:
    :param serializer:
    :param cache:
    :return: Latest read version, for comparison
    """

    current_version = await serializer.end_read(version=version)

    # By tracking readers locally, we can vacuum the cache more efficiently
    await cache.decrement_readers(version)
    return current_version


async def _begin_write_model(
    read_version: Version, serializer: VersionSerializer, cache: VersionCache
) -> Version:
    """

    :param read_version:
    :param serializer:
    :param cache:
    :return: Full write version
    """

    v = await serializer.begin_write(
        read_version=read_version,
        model=True,
        reference=False,
        cube=False,
        operations=False,
        website=False,
    )
    return v

    # TODO
    #  What do we cache and how?
    #  Do we cache uncommitted versions, marked as such?


async def _begin_write_reference_data(
    read_version: Version, serializer: VersionSerializer, cache: VersionCache
) -> Version:
    """

    :param read_version:
    :param serializer:
    :param cache:
    :return: Full write version
    """

    return await serializer.begin_write(
        read_version=read_version,
        model=False,
        reference=True,
        cube=False,
        operations=False,
        website=False,
    )

    # TODO
    #  What do we cache and how?
    #  Do we cache uncommitted versions, marked as such?
    #  REFACTOR with the other begin writes


async def _abort(
    version: Version, serializer: VersionSerializer, cache: VersionCache
) -> Version:
    """

    :param read_version:
    :param serializer:
    :param cache:
    :return: Full write version
    """

    current_version = await serializer.abort(version=version)
    await cache.delete(version)
    return current_version


async def _commit(
    version: Version, serializer: VersionSerializer, cache: VersionCache
) -> Version:
    """

    :param version:
    :param serializer:
    :param cache:
    :return: Full write version
    """

    version = await serializer.commit(version=version)
    await cache.put(version)
    return version

from __future__ import annotations

from contextlib import asynccontextmanager

from lunch.base_classes.conductor import Conductor
from lunch.errors.edit_conflict_errors import EditConflictError
from lunch.mvcc.version import Version
from lunch.storage.version_store import VersionStore


class VersionManager(Conductor):
    def __init__(self, storage: VersionStore):
        self._storage = storage

    @asynccontextmanager
    async def write_model_version(self, read_version: Version):
        """
        Context manager for ModelVersion, to ensure that the version is aborted in case of exception

        :param last_known_version:
        """
        version = await _begin_write_model(
            read_version=read_version, storage=self._storage
        )
        try:
            yield version
        except Exception:
            await _abort(version=version, storage=self._storage)
            raise
        else:
            # Commit can also throw an exception
            await _commit(version=version, storage=self._storage)

    @asynccontextmanager
    async def read_version(self):
        """
        Context manager for Version read, to ensure that the version is flagged as released in case of exception
        """
        version = await _begin_read(storage=self._storage)
        try:
            yield version
        finally:
            await _end_read(version=version, storage=self._storage)


async def _begin_read(storage: VersionStore) -> Version:
    """

    :param storage:
    :return: The latest version
    :raises:
    """
    return await storage.begin_read()


async def _end_read(version: Version, storage: VersionStore) -> Version:
    """

    :param storage:
    :return: The latest version
    :raises:
    """
    return await storage.end_read(version)


async def _begin_write_model(read_version: Version, storage: VersionStore) -> Version:
    """ """
    return await storage.begin_write_model(read_version)


async def _commit(version: Version, storage: VersionStore) -> Version:
    """

    :param storage:
    :return: The latest version
    :raises:
    """
    return await storage.commit(version)


async def _abort(version: Version, storage: VersionStore) -> Version:
    """

    :param storage:
    :return: The latest version
    :raises:
    """
    return await storage.abort(version)

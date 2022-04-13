from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.persistence.model_persistor import ModelPersistor
from lunch.storage.cache.model_cache import ModelCache
from lunch.storage.store import Store
from lunch.mvcc.version import Version


class ModelStore(Store):
    """ Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """
    pass

    def __init__(self, serializer: ModelSerializer, cache: ModelCache):
        self._serializer = serializer
        self._cache = cache

    async def get_dimension(self, name: str, version: Version) -> dict:
        """

        :param dimension:
        :param version:
        :return:
        """
        return await _get_dimension(name=name, version=version, serializer=self._serializer, cache=self._cache)

    async def put_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        await _put_dimension(dimension=dimension, version=version, serializer=self._serializer, cache=self._cache)

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache and in the store for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        await _abort_write(version=version, serializer=self._serializer, cache=self._cache)


async def _get_dimension(name:str, version: Version, serializer: ModelSerializer, cache: ModelCache):
    try:
        return await cache.get_dimension(name, version)
    except KeyError:
        dimension = await serializer.get_dimension(name, version)
        await cache.put_dimension(dimension, version)
        return dimension


async def _put_dimension(dimension:dict, version: Version, serializer: ModelSerializer, cache: ModelCache):

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_dimension(dimension, version)
    await cache.put_dimension(dimension, version)


async def _abort_write(version: Version, serializer: ModelSerializer, cache: ModelCache):
    # Clear out half written data in the cache and in the store (in the background)
    await cache.abort_write(version)
    await serializer.abort_write(version)

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

    def __init__(self, serializer: ModelSerializer, cache: ModelCache,  persistor: ModelPersistor):
        self._serializer = serializer
        self._cache = cache
        self._persistor = persistor

    async def get_dimension(self, name: str, version: Version) -> dict:
        """

        :param dimension:
        :param version:
        :return:
        """
        return await _get_dimension(name=name, version=version, serializer=self._serializer, cache=self._cache, persistor=self._persistor)

    async def put_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        await _put_dimension(dimension=dimension, version=version, serializer=self._serializer, cache=self._cache, persistor=self._persistor)

    async def cache_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        await _put_dimension(dimension=dimension, version=version, serializer=self._serializer, cache=self._cache, persistor=self._persistor)

async def _get_dimension(name:str, version: Version, serializer: ModelSerializer, cache: ModelCache,  persistor : ModelPersistor):
    raise NotImplementedError()

async def _put_dimension(dimension:dict, version: Version, serializer: ModelSerializer, cache: ModelCache,  persistor : ModelPersistor):
    raise NotImplementedError()

async def _cache_dimension(dimension: dict, version: Version, serializer: ModelSerializer, cache: ModelCache):
    raise NotImplementedError()
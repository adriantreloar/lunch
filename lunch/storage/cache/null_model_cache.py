from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache


class NullModelCache(ModelCache):
    """ModelCache which does nothing - thus we'll always end up going to the Serializer"""

    async def get_dimension_id(self, name: str, version: Version) -> dict:
        """

        :param name:
        :param version:
        :return:
        """
        raise KeyError((name, version))

    async def get_dimension(self, id: int, version: Version) -> dict:
        """

        :param id:
        :param version:
        :return:
        """
        raise KeyError((id, version))

    async def put_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        pass

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

    async def get_max_dimension_id(self, version: Version):
        raise KeyError(version)

    async def put_dimension_id(self, dimension_id: int, name: str, version: Version):
        pass

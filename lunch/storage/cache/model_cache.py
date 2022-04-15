from lunch.storage.cache.cache import Cache
from lunch.mvcc.version import Version

class ModelCache(Cache):
    """

    """

    async def get_dimension(self, name: str, version: Version) -> dict:
        """

        :param name:
        :param version:
        :return:
        """
        raise NotImplementedError("Abstract")

    async def put_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        raise NotImplementedError("Abstract")

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        raise NotImplementedError("Abstract")
from lunch.storage.cache.cache import Cache
from lunch.mvcc.version import Version

class VersionCache(Cache):
    """

    """

    async def get_full_version(self, version: int) -> Version:
        """

        :param version: main version number
        :return:
        """
        raise NotImplementedError("Abstract")

    async def put(self, version: Version):
        raise NotImplementedError("Abstract")

    async def delete(self, version: Version):
        raise NotImplementedError("Abstract")


from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.cache import Cache


class VersionCache(Cache):
    """ """

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

    async def increment_readers(self, version: Version):
        raise NotImplementedError("Abstract")

    async def decrement_readers(self, version: Version):
        raise NotImplementedError("Abstract")

from lunch.mvcc.version import Version
from lunch.storage.cache.cache import Cache


class FactDataCache(Cache):
    """ """

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        """
        raise NotImplementedError("Abstract")

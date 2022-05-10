from lunch.mvcc.version import Version
from lunch.storage.cache.cache import Cache


class DimensionDataCache(Cache):
    """ """

    #async def get_dimension(self, id_: int, version: Version) -> dict:
    #    raise NotImplementedError("Abstract")

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        """
        raise NotImplementedError("Abstract")
from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.cache import Cache


class ReferenceDataCache(Cache):
    """ """

    # async def get_dimension(self, id_: int, version: Version) -> dict:
    #    raise NotImplementedError("Abstract")

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        """
        raise NotImplementedError("Abstract")

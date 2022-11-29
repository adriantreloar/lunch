from lunch.mvcc.version import Version
from lunch.storage.cache.fact_data_cache import FactDataCache


class NullFactDataCache(FactDataCache):
    """FactDataCache which does nothing - thus we'll always end up going to the Serializer"""

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

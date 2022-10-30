from lunch.mvcc.version import Version
from lunch.storage.cache.reference_data_cache import ReferenceDataCache


class NullReferenceDataCache(ReferenceDataCache):
    """ReferenceDataCache which does nothing - thus we'll always end up going to the Serializer"""

    # async def get_dimension_id(self, name: str, version: Version) -> dict:
    #    raise KeyError((name, version))

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

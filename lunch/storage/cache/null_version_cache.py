from lunch.storage.cache.version_cache import VersionCache
from lunch.mvcc.version import Version

class NullVersionCache(VersionCache):
    """
    A Version cache that doesn't cache at all, just raises KeyErrors when asked for anything
    Placeholder for development, and useful friend in testing
    """

    async def get_full_version(self, version: int) -> Version:
        """
        :param version: main version number
        :return:
        """
        raise KeyError(version)

    async def put(self, version: Version):
        pass

    async def delete(self, version: Version):
        pass

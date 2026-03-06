from typing import Mapping

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.fact_data_cache import FactDataCache


class NullFactDataCache(FactDataCache):
    """FactDataCache which does nothing - thus we'll always end up going to the Serializer"""

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

    async def get_version_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_version_index(self, index_: dict, version: Version) -> None:
        pass

    async def get_partition_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_partition_index(self, index_: dict, version: Version) -> None:
        pass

    async def get_columns(self, fact_id: int, cube_data_version: int) -> Mapping:
        raise KeyError(fact_id)

    async def put_columns(self, fact_id: int, columns: Mapping, version: Version) -> None:
        pass

from typing import Iterable, Mapping

from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.dimension_data_cache import DimensionDataCache


class NullDimensionDataCache(DimensionDataCache):
    """DimensionDataCache which does nothing - thus we'll always end up going to the Serializer"""

    # async def get_dimension_id(self, name: str, version: Version) -> dict:
    #    raise KeyError((name, version))

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

    async def get_columns(self, dimension_id: int, reference_data_version: int):

        raise KeyError((dimension_id, reference_data_version))

    async def put_columns(
        self,
        dimension_id: int,
        version: Version,
        columns: Mapping[int, Iterable],
    ):
        pass

    async def get_version_index(self, version: Version) -> dict[int, int]:
        raise KeyError(version)

    async def put_version_index(
        self, index_: dict[int, int], version: Version
    ):
        pass
from typing import Any, Iterable, Mapping

from numpy.typing import DTypeLike

from lunch.mvcc.version import Version
from lunch.storage.cache.dimension_data_cache import DimensionDataCache


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

    async def get_columns(self, dimension_id: int, version: Version):

        raise KeyError((dimension_id, version))

    async def put_columns(
        self,
        dimension_id: int,
        version: Version,
        column_data: Mapping[int, Iterable],
        column_types: Mapping[int, DTypeLike],
    ):
        pass

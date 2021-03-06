from lunch.mvcc.version import Version
from lunch.storage.cache.cache import Cache


class ModelCache(Cache):
    """ """

    async def get_dimension(self, id_: int, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_dimensions(self, dimension: list[dict], version: Version):
        raise NotImplementedError("Abstract")

    async def get_fact(self, id_: int, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_fact(self, fact: dict, version: Version):
        raise NotImplementedError("Abstract")

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        """
        raise NotImplementedError("Abstract")

    async def get_max_dimension_id(self, version):
        raise NotImplementedError("Abstract")

    async def put_dimension_id(self, dimension_id: int, name: str, version: Version):
        raise NotImplementedError("Abstract")

    async def get_dimension_name_index(self, version: Version) -> dict[str, int]:
        raise NotImplementedError("Abstract")

    async def get_dimension_version_index(self, version: Version) -> dict[int, int]:
        raise NotImplementedError("Abstract")

    async def put_dimension_name_index(self, index: dict[str, int], version: Version):
        raise NotImplementedError("Abstract")

    async def put_dimension_version_index(
        self, index: dict[int, int], version: Version
    ):
        raise NotImplementedError("Abstract")

    async def get_max_fact_id(self, version):
        raise NotImplementedError("Abstract")

    async def put_fact_id(self, fact_id: int, name: str, version: Version):
        raise NotImplementedError("Abstract")

    # TODO - get_dimension_index, put_dimension_index

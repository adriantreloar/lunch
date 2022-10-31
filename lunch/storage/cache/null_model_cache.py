from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache


class NullModelCache(ModelCache):
    """ModelCache which does nothing - thus we'll always end up going to the Serializer"""

    async def get_dimension_id(self, name: str, version: Version) -> int:
        raise KeyError((name, version))

    async def get_dimension(self, id_: int, version: Version) -> dict:
        raise KeyError((id, version))

    async def put_dimensions(self, dimensions: list[dict], version: Version):
        pass

    async def get_fact_id(self, name: str, version: Version) -> int:
        raise KeyError((name, version))

    async def get_fact(self, id_: int, version: Version) -> dict:
        raise KeyError((id, version))

    async def put_fact(self, fact: dict, version: Version):
        pass

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        pass

    async def get_dimension_name_index(self, version: Version) -> dict[str, int]:
        raise KeyError(version)

    async def get_dimension_version_index(self, version: Version) -> dict[int, int]:
        raise KeyError(version)

    async def put_dimension_name_index(self, index_: dict[str, int], version: Version):
        pass

    async def put_dimension_version_index(
        self, index_: dict[int, int], version: Version
    ):
        pass

    async def get_max_dimension_id(self, version: Version):
        raise KeyError(version)

    async def put_dimension_id(self, dimension_id: int, name: str, version: Version):
        pass

    async def get_max_fact_id(self, version):
        raise KeyError(version)

    async def put_fact_id(self, fact_id: int, name: str, version: Version):
        pass

    async def get_fact_name_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_fact_name_index(self, index_: dict, version: Version):
        pass

    async def get_fact_version_index(self, version: Version) -> dict:
        raise KeyError(version)

    async def put_fact_version_index(self, index_: dict, version: Version):
        pass

    async def put_facts(self, dimensions: list[dict], version: Version):
        pass

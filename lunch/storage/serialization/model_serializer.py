from lunch.mvcc.version import Version
from lunch.storage.serialization.serializer import Serializer


class ModelSerializer(Serializer):
    async def get_dimension_id(self, name: str, version: Version) -> int:
        raise NotImplementedError("Abstract")

    async def get_dimension(self, id_: int, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_dimension(self, dimension: dict, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def get_dimension_name_index(self, version: Version) -> dict[str, int]:
        raise NotImplementedError("Abstract")

    async def get_dimension_version_index(self, version: Version) -> dict[int, int]:
        raise NotImplementedError("Abstract")

    async def put_dimension_version_index(
        self, index: dict[int, int], version: Version
    ):
        raise NotImplementedError("Abstract")

    async def put_dimension_name_index(self, index: dict[str, int], version: Version):
        raise NotImplementedError("Abstract")

    async def get_max_dimension_id(self, version) -> int:
        raise NotImplementedError("Abstract")

    async def get_fact_id(self, name: str, version: Version) -> int:
        raise NotImplementedError("Abstract")

    async def get_fact(self, id_: int, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_fact(self, fact: dict, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def get_fact_index(self, version: Version) -> dict:
        raise NotImplementedError("Abstract")

    async def put_fact_index(self, fact_index: dict, version: Version):
        raise NotImplementedError("Abstract")

    async def get_max_fact_id(self, version) -> int:
        raise NotImplementedError("Abstract")

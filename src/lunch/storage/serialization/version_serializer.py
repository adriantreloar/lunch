from src.lunch.mvcc.version import Version
from src.lunch.storage.serialization.serializer import Serializer


class VersionSerializer(Serializer):
    async def begin_read(self) -> Version:
        raise NotImplementedError("Abstract")

    async def end_read(self, version: Version) -> Version:
        raise NotImplementedError("Abstract")

    async def begin_write(
        self,
        read_version: Version,
        model=False,
        reference=False,
        cube=False,
        operations=False,
        website=False,
    ) -> Version:
        raise NotImplementedError("Abstract")

    async def abort(self, version: Version) -> Version:
        raise NotImplementedError("Abstract")

    async def commit(self, version: Version) -> Version:
        raise NotImplementedError("Abstract")

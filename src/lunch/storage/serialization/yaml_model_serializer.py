import yaml

from src.lunch.mvcc.version import Version
from src.lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from src.lunch.storage.serialization.model_serializer import ModelSerializer
from src.lunch.model.fact import Fact


class YamlModelSerializer(ModelSerializer):
    """ """

    def __init__(self, persistor: LocalFileModelPersistor):
        self._persistor = persistor

    async def get_dimension(self, id_: int, model_version: int) -> dict:
        return await _get_dimension(id_, model_version, self._persistor)

    async def put_dimensions(self, dimensions: list[dict], model_version: int):
        await _put_dimensions(dimensions, model_version, self._persistor)

    async def get_dimension_name_index(self, version: Version) -> dict[str, int]:
        return await _get_dimension_name_index(
            version=version, persistor=self._persistor
        )

    async def put_dimension_name_index(self, index_: dict, version: Version):
        return await _put_dimension_name_index(
            index_=index_, version=version, persistor=self._persistor
        )

    async def put_dimension_version_index(
        self, index_: dict[int, int], version: Version
    ):
        return await _put_dimension_version_index(
            index_=index_, version=version, persistor=self._persistor
        )

    async def get_dimension_version_index(self, version: Version) -> dict[int, int]:
        return await _get_dimension_version_index(
            version=version, persistor=self._persistor
        )

    async def get_max_dimension_id(self, version) -> int:
        return await _get_max_dimension_id(version=version, persistor=self._persistor)

    async def get_fact(self, id_: int, version: Version) -> dict:
        return await _get_fact(
            id_=id_,
            version=version,
            persistor=self._persistor,
        )

    async def put_fact(self, fact: dict, version: Version):
        await _put_fact(fact, version, self._persistor)

    async def put_facts(self, dimensions: list[dict], version: Version):
        await _put_facts(dimensions, version, self._persistor)

    async def get_fact_name_index(self, version: Version) -> dict:
        return await _get_fact_name_index(version=version, persistor=self._persistor)

    async def put_fact_name_index(self, fact_index: dict, version: Version):
        return await _put_fact_name_index(
            index_=fact_index, version=version, persistor=self._persistor
        )

    async def get_fact_version_index(self, version: Version) -> dict:
        return await _get_fact_version_index(version=version, persistor=self._persistor)

    async def put_fact_version_index(self, fact_index: dict, version: Version):
        return await _put_fact_version_index(
            index_=fact_index, version=version, persistor=self._persistor
        )

    async def get_fact_id(self, name: str, version: Version) -> int:
        return await _get_fact_id(name=name, version=version, persistor=self._persistor)

    async def get_max_fact_id(self, version) -> int:
        return await _get_max_fact_id(version=version, persistor=self._persistor)


async def _get_dimension(
    id_: int, model_version: int, persistor: LocalFileModelPersistor
):
    if not model_version:
        return {}

    with persistor.open_dimension_file_read(id_=id_, version=model_version) as stream:
        d = yaml.safe_load(stream)
    return d


async def _put_dimensions(
    dimensions: list[dict], model_version: int, persistor: LocalFileModelPersistor
):
    # TODO - this can be done in parallel perhaps
    for dimension in dimensions:
        with persistor.open_dimension_file_write(
            id_=dimension["id_"], version=model_version
        ) as stream:
            yaml.safe_dump(dimension, stream)


async def _get_dimension_name_index(
    version: Version, persistor: LocalFileModelPersistor
):
    if not version.model_version:
        return {}

    try:
        with persistor.open_dimension_name_index_file_read(
            version=version.model_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d


async def _put_dimension_name_index(
    index_: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_dimension_name_index_file_write(
        version=version.model_version
    ) as stream:
        yaml.safe_dump(index_, stream)


async def _put_dimension_version_index(
    index_: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_dimension_version_index_file_write(
        version=version.model_version
    ) as stream:
        yaml.safe_dump(index_, stream)


async def _get_dimension_version_index(
    version: Version, persistor: LocalFileModelPersistor
):
    if not version.model_version:
        return {}

    try:
        with persistor.open_dimension_version_index_file_read(
            version=version.model_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d


async def _get_max_dimension_id(
    version: Version, persistor: LocalFileModelPersistor
) -> int:
    d = await _get_dimension_version_index(version=version, persistor=persistor)
    try:
        return max(d.values())
    except ValueError:
        return 0


async def _get_fact_id(name: str, version: Version, persistor: LocalFileModelPersistor):
    if not version.model_version:
        raise KeyError(name, version.model_version)

    d = await _get_fact_name_index(version=version, persistor=persistor)

    return d[name]


async def _get_fact(id_: int, version: Version, persistor: LocalFileModelPersistor):
    if not version.model_version:
        return {}

    with persistor.open_fact_file_read(
        id_=id_, version=version.model_version
    ) as stream:
        d = yaml.safe_load(stream)
    return d


async def _put_fact(fact: dict, version: Version, persistor: LocalFileModelPersistor):
    with persistor.open_fact_file_write(
        id_=fact["id_"], version=version.model_version
    ) as stream:
        yaml.safe_dump(fact, stream)


async def _put_facts(
    facts: list[Fact], version: Version, persistor: LocalFileModelPersistor
):
    # TODO - this could be done in parallel perhaps?
    for fact in facts:
        with persistor.open_fact_file_write(
            id_=fact.fact_id, version=version.model_version
        ) as stream:
            yaml.safe_dump(fact.serialize(), stream)


async def _get_fact_version_index(version: Version, persistor: LocalFileModelPersistor):
    if not version.model_version:
        return {}

    try:
        with persistor.open_fact_version_index_file_read(
            version=version.model_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d


async def _put_fact_version_index(
    index_: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_fact_version_index_file_write(
        version=version.model_version
    ) as stream:
        yaml.safe_dump(index_, stream)


async def _get_fact_name_index(version: Version, persistor: LocalFileModelPersistor):
    if not version.model_version:
        return {}

    try:
        with persistor.open_fact_name_index_file_read(
            version=version.model_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d


async def _put_fact_name_index(
    index_: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_fact_name_index_file_write(
        version=version.model_version
    ) as stream:
        yaml.safe_dump(index_, stream)


async def _get_max_fact_id(version: Version, persistor: LocalFileModelPersistor) -> int:
    d = await _get_fact_version_index(version=version, persistor=persistor)
    try:
        return max(d.values())
    except ValueError:
        return 0

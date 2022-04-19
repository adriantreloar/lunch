import yaml

from lunch.mvcc.version import Version
from lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from lunch.storage.serialization.model_serializer import ModelSerializer


class YamlModelSerializer(ModelSerializer):
    """ """

    def __init__(self, persistor: LocalFileModelPersistor):
        self._persistor = persistor

    async def get_dimension(self, name: str, version: Version) -> dict:
        """

        :param name: Name of the dimension
        :param version: Full version of the dimension we are getting
        :return: Dictionary containing the dimension
        """
        return await _get_dimension(name, version, self._persistor)

    async def put_dimension(self, dimension: dict, version: Version):
        """

        :param dimension:
        :param version:
        :return:
        """
        await _put_dimension(dimension, version, self._persistor)

    async def get_dimension_index(self, version: Version) -> dict:
        return await _get_dimension_index(version=version, persistor=self._persistor)

    async def put_dimension_index(self, dimension_index: dict, version: Version):
        return await _put_dimension_index(dimension_index=dimension_index, version=version, persistor=self._persistor)

    async def get_dimension_id(self, name: str, version: Version) -> int:
        return await _get_dimension_id(name=name, version=version, persistor=self._persistor)

    async def get_max_dimension_id(self, version) -> int:
        return await _get_max_dimension_id(version=version, persistor=self._persistor)

async def _get_dimension_id(
    name: str, version: Version, persistor: LocalFileModelPersistor
):
    if not version.model_version:
        raise KeyError(name, version.model_version)

    with persistor.open_dimension_index_file_read(
        version=version.model_version
    ) as stream:
        d = yaml.safe_load(stream)

    return d[name]

async def _get_dimension(
    id_: int, version: Version, persistor: LocalFileModelPersistor
):
    if not version.model_version:
        return {}

    with persistor.open_dimension_file_read(
        id_=id_, version=version.model_version
    ) as stream:
        d = yaml.safe_load(stream)
    return d


async def _put_dimension(
    dimension: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_dimension_file_write(
        id_=dimension["id_"], version=version.model_version
    ) as stream:
        yaml.safe_dump(dimension, stream)

async def _get_dimension_index(
    version: Version, persistor: LocalFileModelPersistor
):
    if not version.model_version:
        return {}

    try:
        with persistor.open_dimension_index_file_read(
            version=version.model_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d


async def _put_dimension_index(
    dimension_index: dict, version: Version, persistor: LocalFileModelPersistor
):
    with persistor.open_dimension_index_file_write(
        version=version.model_version
    ) as stream:
        yaml.safe_dump(dimension_index, stream)


async def _get_max_dimension_id(version: Version, persistor: LocalFileModelPersistor) -> int:
    d = await _get_dimension_index(version=version, persistor=persistor)
    try:
        return max(d.values())
    except ValueError:
        return 0
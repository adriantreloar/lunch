from lunch.mvcc.version import Version
from lunch.storage.serialization.model_serializer import ModelSerializer
import yaml
from lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor

class YamlModelSerializer(ModelSerializer):
    """

    """

    def __init__(self, persistor: LocalFileModelPersistor):
        super.__init__(persistor)

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
        return await _put_dimension(dimension, version, self._persistor)


async def _get_dimension(name: str, version: Version, persistor: LocalFileModelPersistor):
    with await persistor.open_dimension_file_read(name=name, version=version.model_version) as stream:
        d = await yaml.load(stream)
    return d


async def _put_dimension(dimension: dict, version: Version, persistor: LocalFileModelPersistor):
    with await persistor.open_dimension_file_write(name=dimension["name"], version=version.model_version) as stream:
        await yaml.dump(dimension, stream)




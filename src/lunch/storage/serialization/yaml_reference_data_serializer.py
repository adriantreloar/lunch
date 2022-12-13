import yaml

from src.lunch.mvcc.version import Version
from src.lunch.storage.persistence.local_file_reference_data_persistor import (
    LocalFileReferenceDataPersistor,
)
from src.lunch.storage.serialization.reference_data_serializer import (
    ReferenceDataSerializer,
)


class YamlReferenceDataSerializer(ReferenceDataSerializer):
    """ """

    def __init__(self, persistor: LocalFileReferenceDataPersistor):
        self._persistor = persistor

    async def put_dimension_data_version_index(
        self, index: dict[int, int], version: Version
    ):
        return await _put_dimension_data_version_index(
            index=index, version=version, persistor=self._persistor
        )

    async def get_dimension_data_version_index(
        self, version: Version
    ) -> dict[int, int]:
        return await _get_dimension_data_version_index(
            version=version, persistor=self._persistor
        )


async def _put_dimension_data_version_index(
    index: dict, version: Version, persistor: LocalFileReferenceDataPersistor
):
    with persistor.open_dimension_data_version_index_file_write(
        version=version.reference_data_version
    ) as stream:
        yaml.safe_dump(index, stream)


async def _get_dimension_data_version_index(
    version: Version, persistor: LocalFileReferenceDataPersistor
):
    if not version.reference_data_version:
        return {}

    try:
        with persistor.open_dimension_data_version_index_file_read(
            version=version.reference_data_version
        ) as stream:
            d = yaml.safe_load(stream)
    except FileNotFoundError:
        raise KeyError(version)
    return d

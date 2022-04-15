from lunch.mvcc.version import Version
from lunch.storage.serialization.version_serializer import VersionSerializer
import yaml
from lunch.storage.persistence.local_file_version_persistor import LocalFileVersionPersistor
from lunch.storage.transformers.versions_transformer import VersionsTransformer
from asyncio import Lock




class YamlVersionSerializer(VersionSerializer):
    """

    """

    def __init__(self, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer):
        self._persistor = persistor
        self._transformer = transformer
        self._lock = Lock()  # The version file lock

    async def begin_read(self) -> Version:
        return await _begin_read(lock=self._lock, persistor=self._persistor, transformer=self._transformer)

    async def end_read(self, version: Version) -> Version:
        return await _end_read(lock=self._lock, version=version, persistor=self._persistor, transformer=self._transformer)

    async def end_read(self, version: Version) -> Version:
        raise NotImplementedError("Abstract")

    async def begin_write(self,
                          read_version: Version,
                          model=False,
                          reference=False,
                          cube=False, operations=False, website=False) -> Version:
        return await _begin_write(lock=self._lock, read_version=read_version, persistor=self._persistor, transformer=self._transformer, model=False, reference=False, cube=False, operations=False, website=False)

    async def abort(self, version: Version) -> Version:
        return await _abort(lock=self._lock, version=version, persistor= self._persistor, transformer=self._transformer)

    async def commit(self, version: Version) -> Version:
        return await _commit(lock=self._lock, version=version, persistor= self._persistor, transformer=self._transformer)

async def _begin_read(lock: Lock, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer) -> Version:
    # Read, transform, and write version file, in a single atomic operation
    # Obviously this is not going to work in a multiprocessing or multiserver environment
    # For that we'll need a more robust VersionSerializer

    async with lock:

        with persistor.open_version_file_read() as stream:
            version_dict = yaml.safe_load(stream)

        # The first time we read there may be no versions
        if version_dict is None:
            version_dict = {"versions": {0: {"version":Version(version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0), "readers":0, "committed":True, "status":"readable"}}}

        read_version = transformer.get_max_readable_version(version_dict)
        version_dict = transformer.increment_readers_in_versions(version_dict, read_version)

        with persistor.open_version_file_write() as stream:
            yaml.safe_dump(version_dict, stream)

        return write_version

async def _end_read(lock: Lock, read_version: Version, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer,  model=False, reference=False, cube=False, operations=False, website=False) -> Version:
    # Read, transform, and write version file, in a single atomic operation
    # Obviously this is not going to work in a multiprocessing or multiserver environment
    # For that we'll need a more robust VersionSerializer

    async with lock:

        with persistor.open_version_file_read() as stream:
            version_dict = yaml.safe_load(stream)

        version_dict, write_version = transformer.decrement_readers_in_versions(version_dict, read_version)

        with persistor.open_version_file_write() as stream:
            yaml.safe_dump(version_dict, stream)

        return write_version

async def _begin_write(lock: Lock, read_version: Version, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer,  model=False, reference=False, cube=False, operations=False, website=False) -> Version:
    # Read, transform, and write version file, in a single atomic operation
    # Obviously this is not going to work in a multiprocessing or multiserver environment
    # For that we'll need a more robust VersionSerializer

    async with lock:

        with persistor.open_version_file_read() as stream:
            version_dict = yaml.safe_load(stream)

        version_dict, write_version = transformer.start_new_write_version_in_versions(version_dict, read_version, model, reference, cube, operations, website)

        with persistor.open_version_file_write() as stream:
            yaml.safe_dump(version_dict, stream)

        return write_version

async def _abort(lock: Lock, version: Version, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer) -> Version:
    async with lock:
        with persistor.open_version_file_read() as stream:
            version_dict = yaml.safe_load(stream)

        version_dict, latest_read_version = transformer.abort_version_in_versions(version_dict, version)

        with persistor.open_version_file_write() as stream:
            yaml.safe_dump(version_dict, stream)

        return latest_read_version

async def _commit(lock: Lock, version: Version, persistor: LocalFileVersionPersistor, transformer: VersionsTransformer) -> Version:
    async with lock:
        with persistor.open_version_file_read() as stream:
            version_dict = yaml.safe_load(stream)

        version_dict, latest_read_version = transformer.commit_version_in_versions(version_dict, version)

        with persistor.open_version_file_write() as stream:
            yaml.safe_dump(version_dict, stream)

        return latest_read_version

import os.path

from contextlib import contextmanager
from pathlib import Path

from lunch.storage.persistence.reference_data_persistor import ReferenceDataPersistor

class LocalFileReferenceDataPersistor(ReferenceDataPersistor):
    """Hands out open files for file serializers to write to.
    Includes files for reference data indexes, maybe hierarchies.
    Dimensions are a big job, and more variable in format, so these are done separately, in a DimensionDataPersistor.
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def dimension_data_version_index_file(self, version: int) -> Path:
        return _dimension_data_version_index_file(directory=self._directory, version=version)

    @contextmanager
    def open_dimension_data_version_index_file_read(self, version: int):
        file_path = self.dimension_data_version_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_dimension_data_version_index_file_write(self, version: int):
        file_path = self.dimension_data_version_index_file(version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

def _dimension_data_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension_data.version.index.yaml")

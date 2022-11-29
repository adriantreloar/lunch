import os.path
from contextlib import contextmanager
from pathlib import Path

from lunch.storage.persistence.fact_data_persistor import FactDataPersistor


class LocalFileColumnarFactDataPersistor(FactDataPersistor):
    """Hands out open files for file serializers to write to.
    Includes columnar fact data, but not the indices
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data
        """
        self._directory = directory

    def fact_version_index_file(self, version: int) -> Path:
        return _fact_version_index_file(directory=self._directory, version=version)

    def fact_partition_version_index_file(self, fact_id: int, cube_data_version: int) -> Path:
        return _fact_partition_version_index_file(directory=self._directory, fact_id=fact_id, cube_data_version=cube_data_version)

    def fact_data_file(
        self, fact_id: int, dimension_id: int, version: int
    ) -> Path:
        return _fact_data_file(
            fact_id=fact_id,
            dimension_id=dimension_id,
            directory=self._directory,
            version=version,
        )

    @contextmanager
    def open_version_index_file_read(self, version: int):
        file_path = self.fact_version_index_file(version=version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_version_index_file_write(self, version: int):
        file_path = self.fact_version_index_file(version=version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

def _fact_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/fact_data.version.index.yaml")

def _fact_partition_version_index_file(directory: Path, fact_id: int, cube_data_version: int) -> Path:
    return directory.joinpath(f"{cube_data_version}/fact_data/{fact_id}/partition.version.index.yaml")

def _fact_data_file(directory: Path, fact_id: int, cube_data_version: int, partition: tuple[int,]) -> Path:
    partition_directories = '/'.join(partition)
    return directory.joinpath(f"{cube_data_version}/fact_data/{fact_id}/partitions/{partition_directories}/data.yaml")
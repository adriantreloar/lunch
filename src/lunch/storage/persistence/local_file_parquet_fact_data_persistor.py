import os.path
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.fact_data_persistor import FactDataPersistor


class LocalFileParquetFactDataPersistor(FactDataPersistor):
    """Hands out open files for file serializers to write to.
    Includes parquet fact data, and indices
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data
        """
        self._directory = directory

    def fact_version_index_file(self, cube_data_version: int) -> Path:
        return _fact_version_index_file(directory=self._directory, cube_data_version=cube_data_version)

    def fact_partition_version_index_file(self, fact_id: int, cube_data_version: int) -> Path:
        return _fact_partition_version_index_file(directory=self._directory, fact_id=fact_id, cube_data_version=cube_data_version)

    def fact_data_file(
        self, fact_id: int, cube_data_version: int, partition: tuple[int,]
    ) -> Path:
        return _fact_data_file(
            fact_id=fact_id,
            partition=partition,
            directory=self._directory,
            cube_data_version=cube_data_version,
        )

    @contextmanager
    def open_version_index_file_read(self, cube_data_version: int):
        file_path = self.fact_version_index_file(cube_data_version=cube_data_version)
        with open(file_path, "rb") as f:
            yield f

    @contextmanager
    def open_version_index_file_write(self, cube_data_version: int):
        file_path = self.fact_version_index_file(cube_data_version=cube_data_version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            yield f

    @contextmanager
    def open_partition_version_index_file_read(self, cube_data_version: int, fact_id: int):
        file_path = self.fact_partition_version_index_file(cube_data_version=cube_data_version, fact_id=fact_id)
        with open(file_path, "rb") as f:
            yield f

    @contextmanager
    def open_partition_version_index_file_write(self, cube_data_version: int, fact_id: int):
        file_path = self.fact_partition_version_index_file(cube_data_version=cube_data_version, fact_id=fact_id)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            yield f

    @contextmanager
    def open_data_file_read(self, cube_data_version: int, fact_id: int, partition: tuple[int, ]):
        file_path = self.fact_data_file(cube_data_version=cube_data_version, fact_id=fact_id, partition=partition)
        with open(file_path, "rb") as f:
            yield f

    @contextmanager
    def open_data_file_write(self, cube_data_version: int, fact_id: int, partition: tuple[int, ]):
        file_path = self.fact_data_file(cube_data_version=cube_data_version, fact_id=fact_id, partition=partition)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            yield f

def _fact_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/fact_data.version.index.parquet")

def _fact_partition_version_index_file(directory: Path, fact_id: int, cube_data_version: int) -> Path:
    return directory.joinpath(f"{cube_data_version}/fact_data/{fact_id}/partition.version.index.parquet")

def _fact_data_file(directory: Path, fact_id: int, cube_data_version: int, partition: tuple[int,]) -> Path:
    partition_directories = '/'.join(partition)
    return directory.joinpath(f"{cube_data_version}/fact_data/{fact_id}/partitions/{partition_directories}/data.parquet")
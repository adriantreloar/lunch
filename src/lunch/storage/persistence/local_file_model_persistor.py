import os.path
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.model_persistor import ModelPersistor


class LocalFileModelPersistor(ModelPersistor):
    """Hands out open files for file serializers to write to"""

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def dimension_version_index_file(self, version: int) -> Path:
        return _dimension_version_index_file(directory=self._directory, version=version)

    def dimension_name_index_file(self, version: int) -> Path:
        return _dimension_name_index_file(directory=self._directory, version=version)

    def dimension_file(self, id_: int, version: int) -> Path:
        return _dimension_file(directory=self._directory, id_=id_, version=version)

    def fact_name_index_file(self, version: int) -> Path:
        return _fact_name_index_file(directory=self._directory, version=version)

    def fact_version_index_file(self, version: int) -> Path:
        return _fact_version_index_file(directory=self._directory, version=version)

    def fact_file(self, id_: int, version: int) -> Path:
        return _fact_file(directory=self._directory, id_=id_, version=version)

    @contextmanager
    def open_dimension_version_index_file_read(self, version: int):
        file_path = self.dimension_version_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_dimension_version_index_file_write(self, version: int):
        file_path = self.dimension_version_index_file(version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_dimension_name_index_file_read(self, version: int):
        file_path = self.dimension_name_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_dimension_name_index_file_write(self, version: int):
        file_path = self.dimension_name_index_file(version)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_dimension_file_read(self, id_: int, version: int):
        file_path = self.dimension_file(id_, version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_dimension_file_write(self, id_: int, version: int):
        file_path = self.dimension_file(id_, version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_fact_version_index_file_read(self, version: int):
        file_path = self.fact_version_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_fact_version_index_file_write(self, version: int):
        file_path = self.fact_version_index_file(version)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_fact_name_index_file_read(self, version: int):
        file_path = self.fact_name_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_fact_name_index_file_write(self, version: int):
        file_path = self.fact_name_index_file(version)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_fact_file_read(self, id_: int, version: int):
        file_path = self.fact_file(id_, version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_fact_file_write(self, id_: int, version: int):
        file_path = self.fact_file(id_, version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f


def _dimension_name_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension.name.index.yaml")


def _dimension_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension.version.index.yaml")


def _dimension_file(directory: Path, id_: int, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension.{id_}.yaml")


def _fact_name_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/fact.name.index.yaml")


def _fact_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/fact.version.index.yaml")


def _fact_file(directory: Path, id_: int, version: int) -> Path:
    return directory.joinpath(f"{version}/fact.{id_}.yaml")

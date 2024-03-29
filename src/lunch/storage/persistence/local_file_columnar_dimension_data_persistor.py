import os.path
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.dimension_data_persistor import DimensionDataPersistor


class LocalFileColumnarDimensionDataPersistor(DimensionDataPersistor):
    """Hands out open files for file serializers to write to.
    Includes columnar dimension data, but not the indices
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def attribute_file(
        self, dimension_id: int, attribute_id: int, version: int
    ) -> Path:
        return _attribute_file(
            dimension_id=dimension_id,
            attribute_id=attribute_id,
            directory=self._directory,
            version=version,
        )

    def dimension_version_index_file(self, version: int) -> Path:
        return _dimension_version_index_file(directory=self._directory, version=version)

    @contextmanager
    def open_attribute_file_read(
        self, dimension_id: int, attribute_id: int, version: int
    ):
        file_path = self.attribute_file(
            dimension_id=dimension_id, attribute_id=attribute_id, version=version
        )
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_attribute_file_write(
        self, dimension_id: int, attribute_id: int, version: int
    ):
        file_path = self.attribute_file(
            dimension_id=dimension_id, attribute_id=attribute_id, version=version
        )
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_version_index_file_read(self, version: int):
        file_path = self.dimension_version_index_file(version=version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_version_index_file_write(self, version: int):
        file_path = self.dimension_version_index_file(version=version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f


def _attribute_file(
    dimension_id: int, attribute_id: int, directory: Path, version: int
) -> Path:
    return directory.joinpath(
        f"{version}/dimension_data/{dimension_id}/attribute.{attribute_id}.column"
    )


def _dimension_version_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension_data.version.index.yaml")

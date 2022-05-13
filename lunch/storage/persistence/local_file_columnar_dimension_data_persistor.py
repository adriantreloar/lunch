import os.path

from contextlib import contextmanager
from pathlib import Path

from lunch.storage.persistence.dimension_data_persistor import DimensionDataPersistor

class LocalFileColumnarDimensionDataPersistor(DimensionDataPersistor):
    """Hands out open files for file serializers to write to.
    Includes columnar dimension data, but not the indices
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def attribute_file(self, attribute_id: int, version: int) -> Path:
        return _attribute_file(attribute_id=attribute_id, directory=self._directory, version=version)

    @contextmanager
    def open_attribute_file_read(self, attribute_id: int, version: int):
        file_path = self.attribute_file(attribute_id=attribute_id, version=version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_attribute_file_write(self, attribute_id: int, version: int):
        file_path = self.attribute_file(attribute_id=attribute_id, version=version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f

def _attribute_file(attribute_id: int, directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/dimension_data/attribute.{attribute_id}.column")

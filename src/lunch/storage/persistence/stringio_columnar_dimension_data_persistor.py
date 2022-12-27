import os.path
from contextlib import contextmanager
from pathlib import Path
import io

from src.lunch.storage.persistence.local_file_columnar_dimension_data_persistor import LocalFileColumnarDimensionDataPersistor


class StringIOColumnarDimensionDataPersistor(LocalFileColumnarDimensionDataPersistor):
    """Hands out open stringio files for file serializers to write to.
    Includes columnar dimension data, but not the indices

    USed for testing round-trip serialisation
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        super(StringIOColumnarDimensionDataPersistor, self).__init__(directory=directory)
        # Hold our stringio files in here, by file name.
        # They will last as long as this class does
        self._files_by_path = {}

    @contextmanager
    def open_attribute_file_read(
        self, dimension_id: int, attribute_id: int, version: int
    ):
        file_path = self.attribute_file(
            dimension_id=dimension_id, attribute_id=attribute_id, version=version
        )
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            # Emulate the errors we'd get when we try to read a non-existent file
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_attribute_file_write(
        self, dimension_id: int, attribute_id: int, version: int
    ):
        file_path = self.attribute_file(
            dimension_id=dimension_id, attribute_id=attribute_id, version=version
        )
        #with open(file_path, "w") as f:
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    @contextmanager
    def open_version_index_file_read(self, version: int):
        file_path = self.dimension_version_index_file(version=version)
        #with open(file_path, "r") as f:
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            # Emulate the errors we'd get when we try to read a non-existent file
            raise FileNotFoundError(file_path)

        yield f
        f.seek(0)

    @contextmanager
    def open_version_index_file_write(self, version: int):
        file_path = self.dimension_version_index_file(version=version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        #with open(file_path, "w") as f:
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)


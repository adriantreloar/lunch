import os.path
from contextlib import contextmanager
from pathlib import Path
import io
from src.lunch.storage.persistence.local_file_reference_data_persistor import LocalFileReferenceDataPersistor

class StringIOReferenceDataPersistor(LocalFileReferenceDataPersistor):
    """Hands out open string io files for file serializers to write to.
    Used for integration testing, so that we can write to 'files'
    without hitting the file system, and causing slow down, and forcing us to delete files after setup or cleardown
    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        super(StringIOReferenceDataPersistor, self).__init__(directory=directory)
        # Hold our stringio files in here, by file name.
        # They will last as long as this class does
        self._files_by_path = {}

    @contextmanager
    def open_dimension_data_version_index_file_read(self, version: int):
        file_path = self.dimension_data_version_index_file(version)
        #with open(file_path, "r") as f:
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            # Emulate the errors we'd get when we try to read a non-existent file
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_dimension_data_version_index_file_write(self, version: int):
        file_path = self.dimension_data_version_index_file(version)
        #with open(file_path, "w") as f:
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)


from contextlib import contextmanager
from pathlib import Path
from lunch.storage.persistence.model_persistor import ModelPersistor
import unicodedata
import re

class LocalFileVersionPersistor(ModelPersistor):
    """ Hands out open files for file serializers to write to


    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for data. e.g. ~/mylunch/data
        """
        self._directory = directory

    def version_file(self) -> Path:
        return _version_file(directory=self._directory)

    @contextmanager
    def open_version_file_read(self):
        file_path = self.version_file()
        with open(file_path, 'rb') as f:
            yield f

    @contextmanager
    async def open_version_file_write(self):
        file_path = self.version_file()
        with open(file_path, 'wb') as f:
            yield f

def _version_file(directory: Path) -> Path:
    return directory.joinpath(f"_version.yaml")




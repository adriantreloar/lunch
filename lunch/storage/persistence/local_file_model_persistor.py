from contextlib import contextmanager
from pathlib import Path
from lunch.storage.persistence.model_persistor import ModelPersistor
import unicodedata
import re

def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

class LocalFileModelPersistor(ModelPersistor):
    """ Hands out open files for file serializers to write to


    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def index_file(self, version: int) -> Path:
        return _index_file(directory=self._directory, version=version)

    def dimension_file(self, name: str, version: int) -> Path:
        return _dimension_file(directory=self._directory, name=name, version=version)

    @contextmanager
    def open_index_file_read(self, version: int):
        file_path = self.index_file(version)
        with open(file_path, 'rb') as f:
            yield f

    @contextmanager
    async def open_index_file_write(self, version: int):
        file_path = self.index_file(version)
        with open(file_path, 'wb') as f:
            yield f

    @contextmanager
    def open_dimension_file_read(self, name: str, version: int):
        file_path = self.dimension_file(name, version)
        with open(file_path, 'rb') as f:
            yield f

    @contextmanager
    def open_dimension_file_write(self, name: str, version: int):
        file_path = self.dimension_file(name, version)
        with open(file_path, 'wb') as f:
            yield f


def _index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/_index.yaml")


def _dimension_file(directory: Path, name: str, version: int) -> Path:
    return directory.joinpath(f"{version}/{slugify(name)}.yaml")


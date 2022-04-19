import os.path
import re
import unicodedata
from contextlib import contextmanager
from pathlib import Path

from lunch.storage.persistence.model_persistor import ModelPersistor


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
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


class LocalFileModelPersistor(ModelPersistor):
    """Hands out open files for file serializers to write to"""

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    def dimension_index_file(self, version: int) -> Path:
        return _dimension_index_file(directory=self._directory, version=version)

    def dimension_file(self, id_: int, version: int) -> Path:
        return _dimension_file(directory=self._directory, id_=id_, version=version)

    def fact_index_file(self, version: int) -> Path:
        return _fact_index_file(directory=self._directory, version=version)

    def fact_file(self, id_: int, version: int) -> Path:
        return _fact_file(directory=self._directory, id_=id_, version=version)

    @contextmanager
    def open_dimension_index_file_read(self, version: int):
        file_path = self.dimension_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_dimension_index_file_write(self, version: int):
        file_path = self.dimension_index_file(version)
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
    def open_fact_index_file_read(self, version: int):
        file_path = self.fact_index_file(version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_fact_index_file_write(self, version: int):
        file_path = self.fact_index_file(version)
        with open(file_path, "w") as f:
            yield f

    @contextmanager
    def open_fact_file_read(self, id_: int, version: int):
        file_path = self.fact_file(id_, version)
        with open(file_path, "r") as f:
            yield f

    @contextmanager
    def open_fact_file_write(self, name: str, version: int):
        file_path = self.fact_file(name, version)
        Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            yield f


def _dimension_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/_dimension_index.yaml")


def _dimension_file(directory: Path, id_: int, version: int) -> Path:
    return directory.joinpath(f"{version}/{id_}.yaml")


def _fact_index_file(directory: Path, version: int) -> Path:
    return directory.joinpath(f"{version}/_fact_index.yaml")


def _fact_file(directory: Path, id_: int, version: int) -> Path:
    return directory.joinpath(f"{version}/{id_}.yaml")

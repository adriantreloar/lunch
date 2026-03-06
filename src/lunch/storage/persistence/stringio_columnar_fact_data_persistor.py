import io
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.local_file_columnar_fact_data_persistor import LocalFileColumnarFactDataPersistor


class StringIOColumnarFactDataPersistor(LocalFileColumnarFactDataPersistor):
    """In-memory fact-data persistor for testing.

    Replaces every file read/write with a StringIO buffer keyed by the path
    the local-file version would have used.  No files are created on disk.
    """

    def __init__(self, directory: Path):
        super().__init__(directory=directory)
        self._files_by_path = {}

    def _fact_attribute_file_path(self, dimension_id: int, attribute_id: int, version: int) -> Path:
        return self._directory / f"{version}/fact_data/{dimension_id}/column.{attribute_id}.column"

    # ------------------------------------------------------------------ #
    # Version index
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_version_index_file_read(self, version: int):
        file_path = self.fact_version_index_file(version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_version_index_file_write(self, version: int):
        file_path = self.fact_version_index_file(version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Attribute (column) files
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_attribute_file_read(self, dimension_id: int, attribute_id: int, version: int):
        file_path = self._fact_attribute_file_path(dimension_id, attribute_id, version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_attribute_file_write(self, dimension_id: int, attribute_id: int, version: int):
        file_path = self._fact_attribute_file_path(dimension_id, attribute_id, version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

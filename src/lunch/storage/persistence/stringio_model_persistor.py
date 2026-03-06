import io
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor


class StringIOModelPersistor(LocalFileModelPersistor):
    """In-memory model persistor for testing.

    Replaces every file read/write with a StringIO buffer keyed by the path
    the local-file version would have used.  No files are created on disk.
    """

    def __init__(self, directory: Path):
        super().__init__(directory=directory)
        self._files_by_path = {}

    # ------------------------------------------------------------------ #
    # Dimension version index
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_dimension_version_index_file_read(self, version: int):
        file_path = self.dimension_version_index_file(version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_dimension_version_index_file_write(self, version: int):
        file_path = self.dimension_version_index_file(version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Dimension name index
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_dimension_name_index_file_read(self, version: int):
        file_path = self.dimension_name_index_file(version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_dimension_name_index_file_write(self, version: int):
        file_path = self.dimension_name_index_file(version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Dimension file
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_dimension_file_read(self, id_: int, version: int):
        file_path = self.dimension_file(id_, version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_dimension_file_write(self, id_: int, version: int):
        file_path = self.dimension_file(id_, version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Fact version index
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_fact_version_index_file_read(self, version: int):
        file_path = self.fact_version_index_file(version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_fact_version_index_file_write(self, version: int):
        file_path = self.fact_version_index_file(version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Fact name index
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_fact_name_index_file_read(self, version: int):
        file_path = self.fact_name_index_file(version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_fact_name_index_file_write(self, version: int):
        file_path = self.fact_name_index_file(version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

    # ------------------------------------------------------------------ #
    # Fact file
    # ------------------------------------------------------------------ #

    @contextmanager
    def open_fact_file_read(self, id_: int, version: int):
        file_path = self.fact_file(id_, version)
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_fact_file_write(self, id_: int, version: int):
        file_path = self.fact_file(id_, version)
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

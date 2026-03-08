import io
from contextlib import contextmanager

from src.lunch.storage.persistence.hierarchy_data_persistor import HierarchyDataPersistor


class StringIOHierarchyDataPersistor(HierarchyDataPersistor):
    """In-memory persistor for hierarchy data — used in tests."""

    def __init__(self):
        self._files = {}

    @contextmanager
    def open_pairs_file_read(self, dimension_id: int, version: int):
        key = ("pairs", dimension_id, version)
        try:
            f = self._files[key]
        except KeyError:
            raise FileNotFoundError(key)
        yield f
        f.seek(0)

    @contextmanager
    def open_pairs_file_write(self, dimension_id: int, version: int):
        key = ("pairs", dimension_id, version)
        f = io.StringIO()
        self._files[key] = f
        yield f
        f.seek(0)

    @contextmanager
    def open_version_index_file_read(self, version: int):
        key = ("version_index", version)
        try:
            f = self._files[key]
        except KeyError:
            raise FileNotFoundError(key)
        yield f
        f.seek(0)

    @contextmanager
    def open_version_index_file_write(self, version: int):
        key = ("version_index", version)
        f = io.StringIO()
        self._files[key] = f
        yield f
        f.seek(0)

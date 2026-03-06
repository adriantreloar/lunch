import io
from contextlib import contextmanager
from pathlib import Path

from src.lunch.storage.persistence.local_file_version_persistor import LocalFileVersionPersistor


class StringIOVersionPersistor(LocalFileVersionPersistor):
    """In-memory version persistor for testing.

    Replaces the single version-file read/write with a StringIO buffer.
    No files are created on disk.

    ``_files_by_path`` is initialised *before* ``super().__init__()`` because
    the parent constructor immediately calls ``open_version_file_write()`` to
    create the version file; the overridden method must find the dict in place.
    """

    def __init__(self, directory: Path):
        self._files_by_path = {}
        super().__init__(directory=directory)

    @contextmanager
    def open_version_file_read(self):
        file_path = self.version_file()
        try:
            f = self._files_by_path[file_path]
        except KeyError:
            raise FileNotFoundError(file_path)
        yield f
        f.seek(0)

    @contextmanager
    def open_version_file_write(self):
        file_path = self.version_file()
        f = io.StringIO()
        self._files_by_path[file_path] = f
        yield f
        f.seek(0)

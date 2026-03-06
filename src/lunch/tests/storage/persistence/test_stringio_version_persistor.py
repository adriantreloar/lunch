from pathlib import Path

import pytest

from src.lunch.storage.persistence.stringio_version_persistor import StringIOVersionPersistor

DIRECTORY = Path("/fake/data")


@pytest.fixture()
def persistor():
    return StringIOVersionPersistor(directory=DIRECTORY)


def test_version_file_created_on_init(persistor):
    """The parent __init__ calls open_version_file_write(); the buffer must exist."""
    with persistor.open_version_file_read() as f:
        assert f.read() == ""


def test_version_file_roundtrip(persistor):
    with persistor.open_version_file_write() as f:
        f.write("version: 3")
    with persistor.open_version_file_read() as f:
        assert f.read() == "version: 3"


def test_version_file_write_replaces_content(persistor):
    with persistor.open_version_file_write() as f:
        f.write("version: 1")
    with persistor.open_version_file_write() as f:
        f.write("version: 2")
    with persistor.open_version_file_read() as f:
        assert f.read() == "version: 2"


def test_read_is_repeatable(persistor):
    """Multiple reads of the same buffer must each return from position 0."""
    with persistor.open_version_file_write() as f:
        f.write("version: 7")
    with persistor.open_version_file_read() as f:
        first = f.read()
    with persistor.open_version_file_read() as f:
        second = f.read()
    assert first == second == "version: 7"

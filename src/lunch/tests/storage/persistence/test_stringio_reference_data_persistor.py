import pytest
from pathlib import Path

from src.lunch.storage.persistence.stringio_reference_data_persistor import StringIOReferenceDataPersistor

DIRECTORY = Path("/fake/reference_data")


@pytest.fixture()
def persistor():
    return StringIOReferenceDataPersistor(directory=DIRECTORY)


def test_dimension_data_version_index_roundtrip(persistor):
    with persistor.open_dimension_data_version_index_file_write(version=1) as f:
        f.write("dim_data_version_index: 1")
    with persistor.open_dimension_data_version_index_file_read(version=1) as f:
        assert f.read() == "dim_data_version_index: 1"


def test_read_missing_raises_file_not_found(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_dimension_data_version_index_file_read(version=99):
            pass


def test_versions_are_independent(persistor):
    with persistor.open_dimension_data_version_index_file_write(version=1) as f:
        f.write("v1")
    with persistor.open_dimension_data_version_index_file_write(version=2) as f:
        f.write("v2")
    with persistor.open_dimension_data_version_index_file_read(version=1) as f:
        assert f.read() == "v1"
    with persistor.open_dimension_data_version_index_file_read(version=2) as f:
        assert f.read() == "v2"


def test_read_is_repeatable(persistor):
    with persistor.open_dimension_data_version_index_file_write(version=1) as f:
        f.write("content")
    with persistor.open_dimension_data_version_index_file_read(version=1) as f:
        first = f.read()
    with persistor.open_dimension_data_version_index_file_read(version=1) as f:
        second = f.read()
    assert first == second == "content"

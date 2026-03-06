import pytest
from pathlib import Path

from src.lunch.storage.persistence.stringio_columnar_fact_data_persistor import StringIOColumnarFactDataPersistor

DIRECTORY = Path("/fake/data")


@pytest.fixture()
def persistor():
    return StringIOColumnarFactDataPersistor(directory=DIRECTORY)


# ------------------------------------------------------------------ #
# Version index
# ------------------------------------------------------------------ #

def test_version_index_roundtrip(persistor):
    with persistor.open_version_index_file_write(version=1) as f:
        f.write("fact_version_index: 1")
    with persistor.open_version_index_file_read(version=1) as f:
        assert f.read() == "fact_version_index: 1"


def test_version_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_version_index_file_read(version=99):
            pass


def test_version_index_versions_are_independent(persistor):
    with persistor.open_version_index_file_write(version=1) as f:
        f.write("v1")
    with persistor.open_version_index_file_write(version=2) as f:
        f.write("v2")
    with persistor.open_version_index_file_read(version=1) as f:
        assert f.read() == "v1"
    with persistor.open_version_index_file_read(version=2) as f:
        assert f.read() == "v2"


# ------------------------------------------------------------------ #
# Attribute (column) files
# ------------------------------------------------------------------ #

def test_attribute_file_roundtrip(persistor):
    with persistor.open_attribute_file_write(dimension_id=10, attribute_id=0, version=1) as f:
        f.write("col_data")
    with persistor.open_attribute_file_read(dimension_id=10, attribute_id=0, version=1) as f:
        assert f.read() == "col_data"


def test_attribute_file_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_attribute_file_read(dimension_id=99, attribute_id=0, version=1):
            pass


def test_attribute_files_keyed_by_dimension_attribute_and_version(persistor):
    """Different (dimension_id, attribute_id, version) tuples must not collide."""
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=0, version=1) as f:
        f.write("d1_a0_v1")
    with persistor.open_attribute_file_write(dimension_id=2, attribute_id=0, version=1) as f:
        f.write("d2_a0_v1")
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=1, version=1) as f:
        f.write("d1_a1_v1")
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=0, version=2) as f:
        f.write("d1_a0_v2")

    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        assert f.read() == "d1_a0_v1"
    with persistor.open_attribute_file_read(dimension_id=2, attribute_id=0, version=1) as f:
        assert f.read() == "d2_a0_v1"
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=1, version=1) as f:
        assert f.read() == "d1_a1_v1"
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=2) as f:
        assert f.read() == "d1_a0_v2"


def test_read_is_repeatable(persistor):
    """Multiple reads of the same buffer must each return from position 0."""
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=0, version=1) as f:
        f.write("repeated")
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        first = f.read()
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        second = f.read()
    assert first == second == "repeated"

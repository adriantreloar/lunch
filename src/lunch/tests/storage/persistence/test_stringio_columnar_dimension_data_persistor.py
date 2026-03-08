from pathlib import Path

import pytest

from src.lunch.storage.persistence.stringio_columnar_dimension_data_persistor import (
    StringIOColumnarDimensionDataPersistor,
)

DIRECTORY = Path("/fake/dimension_data")


@pytest.fixture()
def persistor():
    return StringIOColumnarDimensionDataPersistor(directory=DIRECTORY)


# ------------------------------------------------------------------ #
# Attribute files
# ------------------------------------------------------------------ #


def test_attribute_file_roundtrip(persistor):
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=0, version=1) as f:
        f.write("col_content")
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        assert f.read() == "col_content"


def test_attribute_file_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_attribute_file_read(dimension_id=99, attribute_id=0, version=1):
            pass


def test_attribute_files_keyed_by_dimension_attribute_and_version(persistor):
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


def test_attribute_read_is_repeatable(persistor):
    with persistor.open_attribute_file_write(dimension_id=1, attribute_id=0, version=1) as f:
        f.write("repeated")
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        first = f.read()
    with persistor.open_attribute_file_read(dimension_id=1, attribute_id=0, version=1) as f:
        second = f.read()
    assert first == second == "repeated"


# ------------------------------------------------------------------ #
# Version index files
# ------------------------------------------------------------------ #


def test_version_index_roundtrip(persistor):
    with persistor.open_version_index_file_write(version=3) as f:
        f.write("version_index: {1: 3}")
    with persistor.open_version_index_file_read(version=3) as f:
        assert f.read() == "version_index: {1: 3}"


def test_version_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_version_index_file_read(version=99):
            pass


def test_version_index_versions_are_independent(persistor):
    with persistor.open_version_index_file_write(version=1) as f:
        f.write("idx_v1")
    with persistor.open_version_index_file_write(version=2) as f:
        f.write("idx_v2")
    with persistor.open_version_index_file_read(version=1) as f:
        assert f.read() == "idx_v1"
    with persistor.open_version_index_file_read(version=2) as f:
        assert f.read() == "idx_v2"


# ------------------------------------------------------------------ #
# Filesystem isolation (bug lunch-hho)
# ------------------------------------------------------------------ #


def test_version_index_write_does_not_touch_filesystem(tmp_path):
    """open_version_index_file_write must not create any directories on disk."""
    non_existent = tmp_path / "should_not_be_created"
    p = StringIOColumnarDimensionDataPersistor(directory=non_existent)
    with p.open_version_index_file_write(version=1) as f:
        f.write("data")
    assert not non_existent.exists()


def test_attribute_file_write_does_not_touch_filesystem(tmp_path):
    """open_attribute_file_write must not create any directories on disk."""
    non_existent = tmp_path / "should_not_be_created"
    p = StringIOColumnarDimensionDataPersistor(directory=non_existent)
    with p.open_attribute_file_write(dimension_id=1, attribute_id=0, version=1) as f:
        f.write("data")
    assert not non_existent.exists()

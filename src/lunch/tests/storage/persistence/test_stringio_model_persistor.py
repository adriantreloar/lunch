from pathlib import Path

import pytest

from src.lunch.storage.persistence.stringio_model_persistor import StringIOModelPersistor

DIRECTORY = Path("/fake/model")


@pytest.fixture()
def persistor():
    return StringIOModelPersistor(directory=DIRECTORY)


# ------------------------------------------------------------------ #
# Dimension version index
# ------------------------------------------------------------------ #


def test_dimension_version_index_roundtrip(persistor):
    with persistor.open_dimension_version_index_file_write(version=1) as f:
        f.write("dim_version_index: 1")
    with persistor.open_dimension_version_index_file_read(version=1) as f:
        assert f.read() == "dim_version_index: 1"


def test_dimension_version_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_dimension_version_index_file_read(version=99):
            pass


# ------------------------------------------------------------------ #
# Dimension name index
# ------------------------------------------------------------------ #


def test_dimension_name_index_roundtrip(persistor):
    with persistor.open_dimension_name_index_file_write(version=2) as f:
        f.write("name_index: {Foo: 1}")
    with persistor.open_dimension_name_index_file_read(version=2) as f:
        assert f.read() == "name_index: {Foo: 1}"


def test_dimension_name_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_dimension_name_index_file_read(version=99):
            pass


# ------------------------------------------------------------------ #
# Dimension file
# ------------------------------------------------------------------ #


def test_dimension_file_roundtrip(persistor):
    with persistor.open_dimension_file_write(id_=3, version=1) as f:
        f.write("name: Department")
    with persistor.open_dimension_file_read(id_=3, version=1) as f:
        assert f.read() == "name: Department"


def test_dimension_file_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_dimension_file_read(id_=99, version=1):
            pass


def test_dimension_files_are_keyed_by_id_and_version(persistor):
    """Separate ids and versions must not collide."""
    with persistor.open_dimension_file_write(id_=1, version=1) as f:
        f.write("dim_1_v1")
    with persistor.open_dimension_file_write(id_=2, version=1) as f:
        f.write("dim_2_v1")
    with persistor.open_dimension_file_write(id_=1, version=2) as f:
        f.write("dim_1_v2")

    with persistor.open_dimension_file_read(id_=1, version=1) as f:
        assert f.read() == "dim_1_v1"
    with persistor.open_dimension_file_read(id_=2, version=1) as f:
        assert f.read() == "dim_2_v1"
    with persistor.open_dimension_file_read(id_=1, version=2) as f:
        assert f.read() == "dim_1_v2"


# ------------------------------------------------------------------ #
# Fact version index
# ------------------------------------------------------------------ #


def test_fact_version_index_roundtrip(persistor):
    with persistor.open_fact_version_index_file_write(version=5) as f:
        f.write("fact_version_index: 5")
    with persistor.open_fact_version_index_file_read(version=5) as f:
        assert f.read() == "fact_version_index: 5"


def test_fact_version_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_fact_version_index_file_read(version=99):
            pass


# ------------------------------------------------------------------ #
# Fact name index
# ------------------------------------------------------------------ #


def test_fact_name_index_roundtrip(persistor):
    with persistor.open_fact_name_index_file_write(version=3) as f:
        f.write("fact_name_index: {Sales: 1}")
    with persistor.open_fact_name_index_file_read(version=3) as f:
        assert f.read() == "fact_name_index: {Sales: 1}"


def test_fact_name_index_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_fact_name_index_file_read(version=99):
            pass


# ------------------------------------------------------------------ #
# Fact file
# ------------------------------------------------------------------ #


def test_fact_file_roundtrip(persistor):
    with persistor.open_fact_file_write(id_=7, version=2) as f:
        f.write("name: Sales")
    with persistor.open_fact_file_read(id_=7, version=2) as f:
        assert f.read() == "name: Sales"


def test_fact_file_read_missing_raises(persistor):
    with pytest.raises(FileNotFoundError):
        with persistor.open_fact_file_read(id_=99, version=1):
            pass


def test_write_at_same_version_overwrites(persistor):
    """StringIO semantics: a second write replaces the buffer."""
    with persistor.open_fact_file_write(id_=1, version=1) as f:
        f.write("first")
    with persistor.open_fact_file_write(id_=1, version=1) as f:
        f.write("second")
    with persistor.open_fact_file_read(id_=1, version=1) as f:
        assert f.read() == "second"

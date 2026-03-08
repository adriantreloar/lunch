import pytest

from src.lunch.model.dimension.dimension_comparer import DimensionComparer


def _dim(attribute_ids: list[int]) -> dict:
    """Build a minimal dimension dict with the given attribute id_ values."""
    return {
        "name": "Department",
        "attributes": [{"name": f"attr_{i}", "id_": i} for i in attribute_ids],
    }


def test_compare_first_write_no_previous():
    result = DimensionComparer.compare(previous_dimension=None, new_dimension=_dim([1, 2]))
    assert result["removed_attribute_ids"] == set()
    assert result["added_attribute_ids"] == {1, 2}


def test_compare_first_write_no_attributes():
    result = DimensionComparer.compare(previous_dimension=None, new_dimension=_dim([]))
    assert result["removed_attribute_ids"] == set()
    assert result["added_attribute_ids"] == set()


def test_compare_same_attributes_no_changes():
    dim = _dim([1, 2])
    result = DimensionComparer.compare(previous_dimension=dim, new_dimension=_dim([1, 2]))
    assert result["removed_attribute_ids"] == set()
    assert result["added_attribute_ids"] == set()


def test_compare_added_one_attribute():
    result = DimensionComparer.compare(previous_dimension=_dim([1]), new_dimension=_dim([1, 2]))
    assert result["removed_attribute_ids"] == set()
    assert result["added_attribute_ids"] == {2}


def test_compare_removed_one_attribute():
    result = DimensionComparer.compare(previous_dimension=_dim([1, 2]), new_dimension=_dim([1]))
    assert result["removed_attribute_ids"] == {2}
    assert result["added_attribute_ids"] == set()


def test_compare_swapped_one_attribute():
    result = DimensionComparer.compare(previous_dimension=_dim([1, 2]), new_dimension=_dim([2, 3]))
    assert result["removed_attribute_ids"] == {1}
    assert result["added_attribute_ids"] == {3}


def test_compare_all_attributes_replaced():
    result = DimensionComparer.compare(previous_dimension=_dim([1, 2]), new_dimension=_dim([3, 4]))
    assert result["removed_attribute_ids"] == {1, 2}
    assert result["added_attribute_ids"] == {3, 4}


def test_compare_no_attributes_field_in_previous():
    previous = {"name": "Department"}
    result = DimensionComparer.compare(previous_dimension=previous, new_dimension=_dim([1]))
    assert result["removed_attribute_ids"] == set()
    assert result["added_attribute_ids"] == {1}

from src.lunch.model.fact import (
    Fact,
    FactDimensionMetadatum,
    FactMeasureMetadatum,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.old_fact.fact_comparer import FactComparer


def _fact(dim_ids: list[int]) -> Fact:
    """Build a minimal Fact with the given dimension_ids."""
    dims = _FactDimensionsMetadata(
        [
            FactDimensionMetadatum(
                name=f"dim_{i}",
                view_order=idx + 1,
                column_id=idx,
                dimension_id=i,
            )
            for idx, i in enumerate(dim_ids)
        ]
    )
    return Fact(name="Sales", dimensions=dims, measures=_FactMeasuresMetadata([]))


def test_compare_first_write_no_previous():
    result = FactComparer.compare(previous_fact=None, new_fact=_fact([1, 2]))
    assert result["removed_dimension_ids"] == set()
    assert result["added_dimension_ids"] == {1, 2}


def test_compare_first_write_no_dimensions():
    result = FactComparer.compare(previous_fact=None, new_fact=_fact([]))
    assert result["removed_dimension_ids"] == set()
    assert result["added_dimension_ids"] == set()


def test_compare_same_dimensions_no_changes():
    fact = _fact([1, 2])
    result = FactComparer.compare(previous_fact=fact, new_fact=_fact([1, 2]))
    assert result["removed_dimension_ids"] == set()
    assert result["added_dimension_ids"] == set()


def test_compare_added_one_dimension():
    result = FactComparer.compare(previous_fact=_fact([1]), new_fact=_fact([1, 2]))
    assert result["removed_dimension_ids"] == set()
    assert result["added_dimension_ids"] == {2}


def test_compare_removed_one_dimension():
    result = FactComparer.compare(previous_fact=_fact([1, 2]), new_fact=_fact([1]))
    assert result["removed_dimension_ids"] == {2}
    assert result["added_dimension_ids"] == set()


def test_compare_swapped_one_dimension():
    result = FactComparer.compare(previous_fact=_fact([1, 2]), new_fact=_fact([2, 3]))
    assert result["removed_dimension_ids"] == {1}
    assert result["added_dimension_ids"] == {3}


def test_compare_all_dimensions_replaced():
    result = FactComparer.compare(previous_fact=_fact([1, 2]), new_fact=_fact([3, 4]))
    assert result["removed_dimension_ids"] == {1, 2}
    assert result["added_dimension_ids"] == {3, 4}


def test_compare_dimension_removed_from_non_empty_to_empty():
    result = FactComparer.compare(previous_fact=_fact([1, 2, 3]), new_fact=_fact([]))
    assert result["removed_dimension_ids"] == {1, 2, 3}
    assert result["added_dimension_ids"] == set()

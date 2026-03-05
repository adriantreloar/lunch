import pytest

from src.lunch.errors.validation_errors import FactValidationError
from src.lunch.model.old_fact.fact_reference_validator import FactReferenceValidator


def test_validate_no_removals_returns_true():
    comparison = {"removed_dimension_ids": set(), "added_dimension_ids": {1}}
    assert FactReferenceValidator.validate(comparison) is True


def test_validate_empty_comparison_returns_true():
    assert FactReferenceValidator.validate({}) is True


def test_validate_both_sets_empty_returns_true():
    comparison = {"removed_dimension_ids": set(), "added_dimension_ids": set()}
    assert FactReferenceValidator.validate(comparison) is True


def test_validate_raises_on_single_removed_dimension():
    comparison = {"removed_dimension_ids": {3}, "added_dimension_ids": set()}
    with pytest.raises(FactValidationError):
        FactReferenceValidator.validate(comparison)


def test_validate_error_message_contains_removed_dimension_id():
    comparison = {"removed_dimension_ids": {42}, "added_dimension_ids": set()}
    with pytest.raises(FactValidationError, match="42"):
        FactReferenceValidator.validate(comparison)


def test_validate_raises_on_multiple_removed_dimensions():
    comparison = {"removed_dimension_ids": {1, 2, 3}, "added_dimension_ids": set()}
    with pytest.raises(FactValidationError):
        FactReferenceValidator.validate(comparison)


def test_validate_raises_even_when_dimensions_also_added():
    # Adding new dimensions does not excuse removing existing ones
    comparison = {"removed_dimension_ids": {1}, "added_dimension_ids": {2}}
    with pytest.raises(FactValidationError):
        FactReferenceValidator.validate(comparison)

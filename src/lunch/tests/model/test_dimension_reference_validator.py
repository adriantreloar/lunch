import pytest

from src.lunch.errors.validation_errors import DimensionValidationError
from src.lunch.model.dimension.dimension_reference_validator import DimensionReferenceValidator


def test_validate_no_removed_attributes_returns_true():
    comparison = {"removed_attribute_ids": set(), "added_attribute_ids": {1}}
    assert DimensionReferenceValidator.validate(comparison) is True


def test_validate_empty_comparison_returns_true():
    comparison = {"removed_attribute_ids": set(), "added_attribute_ids": set()}
    assert DimensionReferenceValidator.validate(comparison) is True


def test_validate_removed_one_attribute_raises():
    comparison = {"removed_attribute_ids": {2}, "added_attribute_ids": set()}
    with pytest.raises(DimensionValidationError):
        DimensionReferenceValidator.validate(comparison)


def test_validate_removed_multiple_attributes_raises():
    comparison = {"removed_attribute_ids": {1, 2, 3}, "added_attribute_ids": set()}
    with pytest.raises(DimensionValidationError):
        DimensionReferenceValidator.validate(comparison)


def test_validate_error_message_contains_attribute_ids():
    comparison = {"removed_attribute_ids": {5}, "added_attribute_ids": set()}
    with pytest.raises(DimensionValidationError, match="5"):
        DimensionReferenceValidator.validate(comparison)


def test_validate_missing_removed_key_returns_true():
    comparison = {"added_attribute_ids": {1}}
    assert DimensionReferenceValidator.validate(comparison) is True

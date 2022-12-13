import pytest

from src.lunch.errors.validation_errors import VersionInternalsValidationError
from src.lunch.mvcc.version import Version


def test_version_equality():
    assert Version(
        version=0,
        model_version=0,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    ) == Version(
        version=0,
        model_version=0,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )

    assert not Version(
        version=0,
        model_version=0,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    ) == Version(
        version=1,
        model_version=1,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )


def test_version_equality_mismatch_raises_error():
    """
    If the version matches, but the innards don't a Validation error should be raised
    """
    with pytest.raises(VersionInternalsValidationError):
        assert Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        ) == Version(
            version=0,
            model_version=1,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        )

    with pytest.raises(VersionInternalsValidationError):
        assert Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        ) == Version(
            version=0,
            model_version=0,
            reference_data_version=1,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        )

    with pytest.raises(VersionInternalsValidationError):
        assert Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        ) == Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=1,
            operations_version=0,
            website_version=0,
        )

    with pytest.raises(VersionInternalsValidationError):
        assert Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        ) == Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=1,
            website_version=0,
        )

    with pytest.raises(VersionInternalsValidationError):
        assert Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=0,
        ) == Version(
            version=0,
            model_version=0,
            reference_data_version=0,
            cube_data_version=0,
            operations_version=0,
            website_version=1,
        )

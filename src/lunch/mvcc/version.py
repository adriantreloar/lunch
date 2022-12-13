from mypy.types import Any

from src.lunch.errors.validation_errors import VersionInternalsValidationError


class Version:
    def __init__(
        self,
        version: int,
        model_version: int,
        reference_data_version: int,
        cube_data_version: int,
        operations_version: int,
        website_version: int,
    ):
        self.version = version
        self.model_version = model_version
        self.reference_data_version = reference_data_version
        self.cube_data_version = cube_data_version
        self.operations_version = operations_version
        self.website_version = website_version

    def __str__(self):
        return str(
            (
                self.version,
                self.model_version,
                self.reference_data_version,
                self.cube_data_version,
                self.operations_version,
                self.website_version,
            )
        )

    def __repr__(self):
        return f"V{self.version}"

    def __eq__(self, other: Any):
        if not isinstance(other, Version):
            return False
        if self.version != other.version:
            return False
        if (
            self.model_version != other.model_version
            or self.reference_data_version != other.reference_data_version
            or self.cube_data_version != other.cube_data_version
            or self.operations_version != other.operations_version
            or self.website_version != other.website_version
        ):
            raise VersionInternalsValidationError(
                f"Two versions with the same version have incompatible sub-versions {self}, {other}"
            )

        return True

    def __hash__(self):
        return self.version


def version_to_dict(version: Version) -> dict:
    return {
        "version": version.version,
        "model_version": version.model_version,
        "reference_data_version": version.reference_data_version,
        "cube_data_version": version.cube_data_version,
        "operations_version": version.operations_version,
        "website_version": version.website_version,
    }


def version_from_dict(version_dict: dict) -> Version:
    return Version(**version_dict)

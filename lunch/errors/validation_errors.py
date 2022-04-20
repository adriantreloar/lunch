class ValidationError(Exception):
    """Exception raised when the structure of a model dict doesn't validate"""

    pass


class VersionInternalsValidationError(ValidationError):
    """Exception raised when the internals of a version don't match up
    e.g. One of the sub-system versions is higher than the main version,
    or the main version matches another version, but the sub-versions don't
    """

    pass


class DimensionValidationError(ValidationError):
    """Exception raised when the structure of a dimension metadata dict doesn't validate"""

    pass

class FactValidationError(ValidationError):
    """Exception raised when the structure of a fact metadata dict doesn't validate"""

    pass

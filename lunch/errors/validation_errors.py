class ValidationError(Exception):
    """Exception raised when the structure of a model dict doesn't validate"""
    pass

class DimensionValidationError(ValidationError):
    """Exception raised when the structure of a dimension dict doesn't validate"""
    pass
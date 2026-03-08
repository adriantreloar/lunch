from src.lunch.base_classes.transformer import Transformer
from src.lunch.errors.validation_errors import DimensionValidationError


class DimensionReferenceValidator(Transformer):
    """Static methods to check referential integrity of a dimension (model)"""

    @staticmethod
    def validate(comparison: dict) -> bool:
        """
        Raises DimensionValidationError if dimension attributes have been removed,
        since existing reference data for those attribute columns would be orphaned.
        """
        removed = comparison.get("removed_attribute_ids", set())
        if removed:
            raise DimensionValidationError(
                f"Cannot remove attributes from a dimension: "
                f"attribute_ids {removed} would orphan existing reference data"
            )
        return True

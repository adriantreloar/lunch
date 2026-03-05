from src.lunch.base_classes.transformer import Transformer
from src.lunch.errors.validation_errors import FactValidationError


class FactReferenceValidator(Transformer):
    """Static methods to check referential integrity of a fact (model)"""

    @staticmethod
    def validate(comparison: dict) -> bool:
        """
        Raises FactValidationError if dimension references have been removed from a fact,
        since existing cube data for those columns would be orphaned.
        """
        removed = comparison.get("removed_dimension_ids", set())
        if removed:
            raise FactValidationError(
                f"Cannot remove dimension references from a fact: "
                f"dimension_ids {removed} would orphan existing cube data"
            )
        return True

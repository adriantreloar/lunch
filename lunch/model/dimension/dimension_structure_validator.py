from lunch.base_classes.transformer import Transformer
from lunch.errors.validation_errors import DimensionValidationError


class DimensionStructureValidator(Transformer):
    """Static methods to check the internal structure of dimension dictionaries"""

    @staticmethod
    def validate(data: dict) -> bool:
        """ """
        try:
            data["name"]
        except KeyError:
            raise DimensionValidationError("No name key")

        return True

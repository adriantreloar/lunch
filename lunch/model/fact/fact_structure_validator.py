from lunch.base_classes.transformer import Transformer
from lunch.errors.validation_errors import FactValidationError


class FactStructureValidator(Transformer):
    """Static methods to check the internal structure of dimension dictionaries"""

    @staticmethod
    def validate(data: dict) -> bool:
        """ """
        for k in ["name", "dimensions", "measures"]:
            try:
                data[k]
            except KeyError:
                raise FactValidationError(k)

        return True

from src.lunch.base_classes.transformer import Transformer


class FactReferenceValidator(Transformer):
    """Static methods to check referential integrity of a fact (model)"""

    @staticmethod
    def validate(data: dict) -> bool:
        """
        A fact can only refer to dimensions that already exist.
        """
        pass

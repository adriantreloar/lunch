from src.lunch.base_classes.transformer import Transformer


class FactComparer(Transformer):
    """Static methods to compare two dimension dictionaries"""

    @staticmethod
    def compare(lhs_fact: dict, rhs_fact: dict) -> dict:
        """
        Create a report detailing the differences between two facts
        """

        return {"changes": "changes"}

from lunch.base_classes.conductor import Conductor
from lunch.managers.model_manager import ModelManager


class FactReferenceValidator(Conductor):
    """Static methods to check referential integrity of a fact (model)"""

    @staticmethod
    def validate(data: dict, model_manager: ModelManager) -> bool:
        """
        A fact can only refer to dimensions that already exist.
        """
        pass

from lunch.base_classes.conductor import Conductor
from lunch.managers.model_manager import ModelManager


class ReferenceValidator(Conductor):
    """ Dynamic methods to check referential integrity of a dimension
    """

    @staticmethod
    def validate(data : dict, model_manager: ModelManager) -> bool:
        """ """
        pass
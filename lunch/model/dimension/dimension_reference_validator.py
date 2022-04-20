from lunch.base_classes.conductor import Conductor
from lunch.managers.model_manager import ModelManager


class DimensionReferenceValidator(Conductor):
    """Static methods to check referential integrity of a dimension (model)

    Note, there aren't currently any references from dimension (model)
    The attributes belong to the dimension, so are defined by it as part of the same document.

    """

    @staticmethod
    def validate(data: dict, model_manager: ModelManager) -> bool:
        """ """
        pass

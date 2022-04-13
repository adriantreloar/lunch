from lunch.base_classes.conductor import PureConductor


class ReferenceValidator(PureConductor):
    """ Dynamic methods to check referential integrity of a dimension
    """

    @staticmethod
    def validate(data : dict, model_manager: ModelManager) -> bool:
        """ """
        pass
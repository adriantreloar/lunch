from lunch.base_classes.transformer import Transformer


class DimensionReferenceValidator(Transformer):
    """Static methods to check referential integrity of a dimension (model)

    Note, there aren't currently any references from dimension (model)
    The attributes belong to the dimension, so are defined by it as part of the same document.

    """

    @staticmethod
    def validate(data: dict) -> bool:
        """ """
        pass

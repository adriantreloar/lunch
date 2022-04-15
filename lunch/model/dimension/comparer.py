from lunch.base_classes.transformer import Transformer


class DimensionComparer(Transformer):
    """ Static methods to compare two dimension dictionaries
    """

    @staticmethod
    def compare(lhs_dimension : dict, rhs_dimension: dict) -> dict:
        """
        Create a report detailing the differences between two dimensions
        """

        return {"changes":"changes"}
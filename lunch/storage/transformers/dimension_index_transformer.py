from lunch.base_classes.transformer import Transformer


class DimensionIndexTransformer(Transformer):
    """
    Transform dimension index dictionaries, when using a very basic read-transform-write serializer.
    """

    @staticmethod
    def get_max_id(dimension_index_dict: dict[str, int]) -> int:
        try:
            return max(dimension_index_dict.keys())
        except KeyError:
            return 0

    @staticmethod
    def add_dimension_name_to_index(
        dimension_index_dict: dict[str, int], name: str
    ) -> tuple[dict, int]:
        """

        :param dimension_index_dict:
        :param name:
        :return: The new id of the dimension
        """
        try:
            return dimension_index_dict, dimension_index_dict[name]
        except KeyError:
            max_id = DimensionIndexTransformer.get_max_id(
                dimension_index_dict=dimension_index_dict
            )

            output_dimension_index_dict = dimension_index_dict.copy()
            output_dimension_index_dict[name] = max_id
            return output_dimension_index_dict, max_id

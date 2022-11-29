from lunch.base_classes.transformer import Transformer
from lunch.mvcc.version import Version


class DimensionModelIndexTransformer(Transformer):
    """
    Transform dimension index dictionaries, when using a very basic read-transform-write serializer.
    """

    # @staticmethod
    # def get_max_id(dimension_index_dict: dict[str, int]) -> int:
    #    try:
    #        return max(dimension_index_dict.values())
    #    except KeyError:
    #        return 0

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
            max_id = DimensionModelIndexTransformer.get_max_id(
                dimension_index_dict=dimension_index_dict
            )

            output_dimension_index_dict = dimension_index_dict.copy()
            output_dimension_index_dict[name] = max_id
            return output_dimension_index_dict, max_id

    @staticmethod
    def update_dimension_version_index(
        index_: dict[int, int], write_version: Version, changed_ids: list[int]
    ) -> dict[int, int]:
        copy_index = index_.copy()
        copy_index.update({id_: write_version.model_version for id_ in changed_ids})
        return copy_index

    @staticmethod
    def update_dimension_name_index(
        index_: dict[str, int], changed_names_index: dict[str, int]
    ) -> dict[str, int]:
        reversed_index = {v: k for k, v in index_.items()}
        reversed_changed_names_index = {v: k for k, v in changed_names_index.items()}
        reversed_index.update(reversed_changed_names_index)
        return {v: k for k, v in reversed_index.items()}

    @staticmethod
    def get_max_id(dimension_index_dict: dict[str, int]) -> int:
        return max(dimension_index_dict.values())

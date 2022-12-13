from src.lunch.base_classes.transformer import Transformer
from src.lunch.mvcc.version import Version


class FactModelIndexTransformer(Transformer):
    """
    Transform dimension index dictionaries, when using a very basic read-transform-write serializer.
    """

    # @staticmethod
    # def get_max_id(fact_index_dict: dict[str, int]) -> int:
    #    try:
    #        return max(fact_index_dict.keys_values())
    #    except KeyError:
    #        return 0

    @staticmethod
    def add_fact_name_to_index(
        fact_index_dict: dict[str, int], name: str
    ) -> tuple[dict, int]:
        """

        :param fact_index_dict:
        :param name:
        :return: The new id of the dimension
        """
        try:
            return fact_index_dict, fact_index_dict[name]
        except KeyError:
            max_id = FactModelIndexTransformer.get_max_id(fact_index_dict=fact_index_dict)

            output_fact_index_dict = fact_index_dict.copy()
            output_fact_index_dict[name] = max_id
            return output_fact_index_dict, max_id

    @staticmethod
    def update_fact_version_index(
        index_: dict[int, int], write_version: Version, changed_ids: list[int]
    ) -> dict[int, int]:
        copy_index = index_.copy()
        copy_index.update({id_: write_version.model_version for id_ in changed_ids})
        return copy_index

    @staticmethod
    def update_fact_name_index(
        index_: dict[str, int], changed_names_index: dict[str, int]
    ) -> dict[str, int]:
        copy_index = index_.copy()
        copy_index.update(changed_names_index)
        return copy_index

    @staticmethod
    def get_max_id(fact_index_dict: dict[str, int]) -> int:
        return max(fact_index_dict.values())

from lunch.base_classes.transformer import Transformer
from lunch.mvcc.version import Version


class FactDataIndexTransformer(Transformer):
    """
    Transform fact data index dictionaries, when using a very basic read-transform-write serializer.
    """

    @staticmethod
    def update_fact_data_version_index(
        index_: dict[int, int], write_version: Version, changed_ids: list[int]
    ) -> dict[int, int]:
        """

        :param index_: A fact data version index - i.e. which fact id's data is at which version
        :param write_version:
        :param changed_ids:
        :return:
        """
        copy_index = index_.copy()
        copy_index.update({id_: write_version.reference_data_version for id_ in changed_ids})
        return copy_index

    @staticmethod
    def get_max_id(fact_version_index_dict: dict[str, int]) -> int:
        return max(fact_version_index_dict.values())

    @staticmethod
    def update_fact_partition_version_index(
        index_: dict[tuple[int, ...], int], write_version: Version, changed_ids: list[tuple[int, ...]]
    ) -> dict[tuple[int, ...], int]:
        """

        :param index_:  A fact partition version index - i.e. which fact partition data is at which version
        :param write_version:
        :param changed_ids: List of partition tuples (created from dimension data ids within the fact)
        :return:
        """
        copy_index = index_.copy()
        copy_index.update({id_: write_version.cube_data_version for id_ in changed_ids})
        return copy_index

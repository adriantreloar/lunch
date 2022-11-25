from lunch.base_classes.transformer import Transformer
from lunch.mvcc.version import Version


class DimensionDataIndexTransformer(Transformer):
    """
    Transform dimension data index dictionaries, when using a very basic read-transform-write serializer.
    """

    @staticmethod
    def update_dimension_version_index(
        index_: dict[int, int], write_version: Version, changed_ids: list[int]
    ) -> dict[int, int]:
        copy_index = index_.copy()
        copy_index.update({id_: write_version.reference_data_version for id_ in changed_ids})
        return copy_index

    @staticmethod
    def get_max_id(dimension_index_dict: dict[str, int]) -> int:
        return max(dimension_index_dict.values())

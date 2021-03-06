from lunch.mvcc.version import Version, version_from_dict, version_to_dict
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)

NOUGHT = Version(
    version=0,
    model_version=0,
    reference_data_version=0,
    cube_data_version=0,
    operations_version=0,
    website_version=0,
)

TEN_NINE_EIGHT = Version(
    version=10,
    model_version=9,
    reference_data_version=8,
    cube_data_version=7,
    operations_version=6,
    website_version=5,
)


def test_add_name_to_empty_names_dict():
    transformer = DimensionIndexTransformer()
    result = transformer.update_dimension_name_index(
        index={}, changed_names_index={"Foo": 1}
    )
    assert result == {"Foo": 1}


def test_add_entry_to_empty_versions_dict():
    transformer = DimensionIndexTransformer()
    result = transformer.update_dimension_version_index(
        index={}, write_version=TEN_NINE_EIGHT, changed_ids=[1, 2, 3]
    )
    assert result == {1: 9, 2: 9, 3: 9}

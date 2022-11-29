from lunch.mvcc.version import Version, version_from_dict, version_to_dict
from lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
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
    transformer = DimensionModelIndexTransformer()
    result = transformer.update_dimension_name_index(
        index_={}, changed_names_index={"Foo": 1}
    )
    assert result == {"Foo": 1}


def test_update_name_in_dict():
    transformer = DimensionModelIndexTransformer()
    result = transformer.update_dimension_name_index(
        index_={"Foo": 1}, changed_names_index={"Bar": 1}
    )
    assert result == {"Bar": 1}


def test_add_entry_to_empty_versions_dict():
    transformer = DimensionModelIndexTransformer()
    result = transformer.update_dimension_version_index(
        index_={}, write_version=TEN_NINE_EIGHT, changed_ids=[1, 2, 3]
    )
    assert result == {1: 9, 2: 9, 3: 9}

from lunch.mvcc.version import Version
from lunch.storage.transformers.versions_transformer import VersionsTransformer

EMPTY = {}


def test_get_max_version_from_empty():
    result = VersionsTransformer.get_max_version(versions_dict=EMPTY)
    assert result == Version(
        version=0,
        model_version=0,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )

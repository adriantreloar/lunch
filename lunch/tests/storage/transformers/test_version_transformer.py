from lunch.mvcc.version import Version, version_from_dict, version_to_dict
from lunch.mvcc.versions_transformer import VersionsTransformer

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


def test_version_to_dict_and_back():
    # Round trip test
    assert version_from_dict(version_to_dict(TEN_NINE_EIGHT)) == TEN_NINE_EIGHT


def test_get_max_version_from_empty():
    result = VersionsTransformer.get_max_version(versions_dict={})
    assert result == NOUGHT


def test_start_new_write_version_on_empty():

    (
        new_versions_dict,
        write_version,
    ) = VersionsTransformer.start_new_write_version_in_versions(
        versions_dict={},
        read_version=NOUGHT,
        model=True,
        reference=False,
        cube=False,
        operations=False,
        website=False,
    )

    NEW_VERSION = Version(
        version=1,
        model_version=1,
        reference_data_version=0,
        cube_data_version=0,
        operations_version=0,
        website_version=0,
    )

    assert write_version == NEW_VERSION
    assert new_versions_dict == {
        "versions": {
            1: {
                "version": version_to_dict(version=NEW_VERSION),
                "committed": False,
                "readers": 0,
                "status": "writing",
            }
        }
    }

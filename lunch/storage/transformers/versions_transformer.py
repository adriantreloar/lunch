from lunch.base_classes.transformer import Transformer
from lunch.mvcc.version import Version, version_from_dict, version_to_dict

class VersionsTransformer(Transformer):
    """
    Transform version storage dictionaries, when using a very basic read-transform-write
    version serializer.

    It is expected (20220414) that a much more sophisticated mechanism will transform the version file,
    since multiprocessing based vacuuming will need to get to the Version file independently.
    Such a process would behave much more like postgres, fiddling with the bytes in a file in atomic byte writes,
    and not reading, transforming and then rewriting the whole file.

    However, this mechanism is good enough while developing the framework, on a single machine.
    """

    @staticmethod
    def get_max_version(versions_dict: dict) -> Version:
        try:
            versions = versions_dict["versions"]
        except KeyError:
            return Version(version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0)

        max_key = max(versions.keys())
        return version_from_dict(versions[max_key]["version"])

    @staticmethod
    def start_new_write_version_in_versions(versions_dict: dict, read_version: Version, model: bool, reference: bool, cube: bool, operations: bool, website: bool) -> tuple[dict, Version]:
        """

        :param versions_dict: dictionary from the versions file - state of the read and write versions
        :param read_version: The full read version we are baselining the write on
        :param model:  true if  write transaction includes a model data update
        :param reference: true if  write transaction includes a reference data update
        :param cube: true if  write transaction includes a cube data update
        :param operations: true if write transaction includes an operations data update
        :param website: true if write transaction includes a website data update
        :returns: tuple of - new versions_dict including the new write version, and the new write_version
        """

        max_version = VersionsTransformer.get_max_version(versions_dict)
        write_version_int = max_version.version + 1
        model_version = write_version_int if model else read_version.model_version
        reference_data_version = write_version_int if reference else read_version.reference_data_version
        cube_data_version = write_version_int if cube else read_version.cube_data_version
        operations_version = write_version_int if operations else read_version.operations_version
        website_version = write_version_int if website else read_version.website_version

        write_version_full = Version(version=write_version_int,
                model_version=model_version,
                reference_data_version=reference_data_version,
                cube_data_version=cube_data_version,
                operations_version=operations_version,
                website_version=website_version
                )

        versions_dict_out = versions_dict.copy()

        try:
            versions = versions_dict_out["versions"]
        except KeyError:
            versions_dict_out["versions"] = {}
            versions = {}

        versions[write_version_int] = {"committed": False, "status": "writing", "version": version_to_dict(write_version_full), "readers": 0}

        return versions_dict_out, write_version_full

    @staticmethod
    def commit_version_in_versions(versions_dict: dict, version: Version) -> dict:

        versions_dict_out = versions_dict.copy()

        committing_version = versions_dict_out["versions"][version.version]
        committing_version.update({"committed": True, "status": "readable"})

        return versions_dict_out

    @staticmethod
    def abort_version_in_versions(versions_dict: dict, version: Version) -> dict:

        versions_dict_out = versions_dict.copy()

        aborting_version = versions_dict_out["versions"][version.version]
        aborting_version.update({"committed": False, "status": "aborted"})

        return versions_dict_out

    @staticmethod
    def get_max_readable_version(versions_dict: dict) -> Version:
        if versions_dict:
            max_version = max(filter(lambda v: v["committed"], versions_dict["versions"].values()),
                              key=lambda v: v["version"]["version"])
            return version_from_dict(max_version["version"])
        else:
            return Version(version=0, model_version=0,reference_data_version=0,cube_data_version=0,operations_version=0,website_version=0)

    @staticmethod
    def increment_readers_in_versions(versions_dict: dict, version: Version) -> Version:
        """
        When doing reads we must increment the number of readers, to prevent live versions being vacuumed

        :param versions_dict:
        :param version:
        :return:
        """
        versions_dict_out = versions_dict.copy()

        incrementing_version = versions_dict_out["versions"][version.version]
        incrementing_version["readers"] += 1

        return versions_dict_out


    @staticmethod
    def decrement_readers_in_versions(versions_dict: dict, version: Version) -> Version:
        """
        When finishing reads we must decrement the number of readers, to allow old versions to be vacuumed

        :param versions_dict:
        :param version:
        :return:
        """
        versions_dict_out = versions_dict.copy()

        decrementing_version = versions_dict_out["versions"][version.version]
        decrementing_version["readers"] -= 1

        return versions_dict_out

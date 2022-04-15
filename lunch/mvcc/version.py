
class Version:

    def __init__(self, version: int, model_version: int, reference_data_version: int, cube_data_version: int, operations_version: int, website_version: int):
        self.version = version
        self.model_version = model_version
        self.reference_data_version = reference_data_version
        self.cube_data_version = cube_data_version
        self.operations_version = operations_version
        self.website_version = website_version

    def __str__(self):
        return str((self.version, self.model_version, self.reference_data_version, self.cube_data_version, self.operations_version, self.website_version))

def version_to_dict(version: Version) -> dict:
    return {"version": version.version,
            "model_version": version.model_version,
            "reference_data_version": version.reference_data_version,
            "cube_data_version": version.cube_data_version,
            "operations_version": version.operations_version,
            "website_version":version.website_version}

def version_from_dict(version_dict: dict) -> Version:
    return Version(**version_dict)

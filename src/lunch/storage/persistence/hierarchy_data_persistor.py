from src.lunch.storage.persistence.persistor import Persistor


class HierarchyDataPersistor(Persistor):
    """Abstract base persistor for hierarchy (parent-child relationship) data."""

    def open_pairs_file_read(self, dimension_id: int, version: int):
        raise NotImplementedError("Abstract")

    def open_pairs_file_write(self, dimension_id: int, version: int):
        raise NotImplementedError("Abstract")

    def open_version_index_file_read(self, version: int):
        raise NotImplementedError("Abstract")

    def open_version_index_file_write(self, version: int):
        raise NotImplementedError("Abstract")

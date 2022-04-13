import os
from contextlib import asynccontextmanager
from pathlib import Path
from lunch.storage.persistence.model_persistor import ModelPersistor

class LocalFileModelPersistor(ModelPersistor):
    """ Hands out open files for file serializers to write to


    """

    def __init__(self, directory: Path):
        """

        :param directory: root directory for model instances. e.g. ~/mylunch/data/model
        """
        self._directory = directory

    @asynccontextmanager
    async def open_dimension_file_read(self, name: str, version: int):
        pass

    @asynccontextmanager
    async def open_dimension_file_write(self, name: str, version: int):
        pass

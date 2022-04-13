from lunch.storage.persistence.model_persistor import ModelPersistor
from lunch.mvcc.version import Version
from serializer import Serializer


class ModelSerializer(Serializer):

    def __init__(self, persistor: ModelPersistor):
        super.__init__(persistor)


    async def get_dimension(self, name: str, version: Version) -> dict:
        raise not NotImplementedError("Abstract")


    async def put_dimension(self, dimension: dict, version: Version):
        raise not NotImplementedError("Abstract")

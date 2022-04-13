from lunch.base_classes.conductor import Conductor
from lunch.storage.persistence.persistor import Persistor

class Serializer(Conductor):
    """Base class for serializers.
    """

    def __init__(self, persistor: Persistor):
        self._persistor = persistor
from lunch.storage.store import Store


class VersionStore(Store):
    """ Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """
    pass

    def __init__(self, serializer: VersionSerializer,  persister : VersionPersistor):
        pass
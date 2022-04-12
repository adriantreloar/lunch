
class ModelStore():
    """ Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """
    pass

    def __init__(self, serializer: ModelSerializer, persister : ModelPersistor):
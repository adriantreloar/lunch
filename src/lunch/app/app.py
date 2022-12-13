class App:
    def __init__(self):
        pass

    # Parse args and config
    # Set up:

    # Persistence
    # Serializers

    # Caches

    # Storage
    # Version Storage (if any - for tests will be in memory, a served version manager needs network setup)
    # Reference Data Storage
    # Cube Data Storage
    # Website data storage

    # Validators, Transformers

    # Managers
    # Version Manager (needs version storage)
    # Model manager (needs version manager, model storage)
    # Reference Data Manager (needs model manager, version manager, reference data storage)
    # Data Operation manager - queries refer to reference data as well as models
    # Cube Data manager (needs model_manager, reference data manager, version manager, cube data storage)
    # Website Manager (needs model_manager, reference data manager, data op manager, version manager, website storage)
    #

    # Set up server(s)

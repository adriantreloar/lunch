from pathlib import Path

from src.lunch.globals.global_state import GlobalState
from src.lunch.managers.model_manager import ModelManager
from src.lunch.managers.version_manager import VersionManager
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import (
    DimensionReferenceValidator,
)
from src.lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator,
)
from src.lunch.model.dimension.dimension_transformer import DimensionTransformer
from src.lunch.mvcc.versions_transformer import VersionsTransformer
from src.lunch.storage.cache.null_model_cache import NullModelCache
from src.lunch.storage.cache.null_version_cache import NullVersionCache
from src.lunch.storage.model_store import ModelStore
from src.lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from src.lunch.storage.persistence.local_file_version_persistor import (
    LocalFileVersionPersistor,
)
from src.lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from src.lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from src.lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
)
from src.lunch.storage.transformers.fact_model_index_transformer import FactModelIndexTransformer
from src.lunch.storage.version_store import VersionStore

# Constant Global State
global_state = GlobalState()

# Validators, Transformers
version_transformer = VersionsTransformer()
dimension_transformer = DimensionTransformer()
dimension_index_transformer = DimensionModelIndexTransformer()
dimension_comparer = DimensionComparer()
dimension_structure_validator = DimensionStructureValidator()
dimension_reference_validator = DimensionReferenceValidator()
fact_index_transformer = FactModelIndexTransformer()


# Persistence
version_persistor = LocalFileVersionPersistor(
    directory=Path("/home/treloarja/PycharmProjects/lunch/example_output")
)
model_persistor = LocalFileModelPersistor(
    directory=Path("/home/treloarja/PycharmProjects/lunch/example_output/model")
)

# Serializers
version_serializer = YamlVersionSerializer(
    persistor=version_persistor, transformer=version_transformer
)
model_serializer = YamlModelSerializer(persistor=model_persistor)

# Caches
version_cache = NullVersionCache()
model_cache = NullModelCache()

# Storage
version_store = VersionStore(serializer=version_serializer, cache=version_cache)
model_store = ModelStore(
    dimension_comparer=dimension_comparer,
    dimension_transformer=dimension_transformer,
    dimension_index_transformer=dimension_index_transformer,
    #fact_transformer=fact_transformer,
    fact_index_transformer=fact_index_transformer,
    #fact_comparer=fact_comparer,
    serializer=model_serializer,
    cache=model_cache,
)

# Managers
version_manager = VersionManager(storage=version_store)
model_manager = ModelManager(
    dimension_structure_validator=dimension_structure_validator,
    dimension_comparer=dimension_comparer,
    dimension_reference_validator=dimension_reference_validator,
    dimension_transformer=dimension_transformer,
    storage=model_store,
    global_state=global_state,
)

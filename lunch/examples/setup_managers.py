from pathlib import Path

from lunch.managers.model_manager import ModelManager
from lunch.managers.version_manager import VersionManager
from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_reference_validator import (
    DimensionReferenceValidator,
)
from lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator,
)
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_comparer import FactComparer
from lunch.model.fact.fact_reference_validator import FactReferenceValidator
from lunch.model.fact.fact_structure_validator import FactStructureValidator
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.versions_transformer import VersionsTransformer
from lunch.storage.cache.null_model_cache import NullModelCache
from lunch.storage.cache.null_version_cache import NullVersionCache
from lunch.storage.model_store import ModelStore
from lunch.storage.reference_data_store import ReferenceDataStore
from lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from lunch.storage.persistence.local_file_version_persistor import (
    LocalFileVersionPersistor,
)
from lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)
from lunch.storage.transformers.fact_index_transformer import FactIndexTransformer
from lunch.storage.version_store import VersionStore

# Validators, Transformers
version_transformer = VersionsTransformer()
dimension_transformer = DimensionTransformer()
dimension_index_transformer = DimensionIndexTransformer()
dimension_comparer = DimensionComparer()
dimension_structure_validator = DimensionStructureValidator()
dimension_reference_validator = DimensionReferenceValidator()
fact_transformer = FactTransformer()
fact_comparer = FactComparer()
fact_structure_validator = FactStructureValidator()
fact_reference_validator = FactReferenceValidator()
fact_index_transformer = FactIndexTransformer()


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
    fact_transformer=fact_transformer,
    fact_index_transformer=fact_index_transformer,
    fact_comparer=fact_comparer,
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
    fact_comparer=fact_comparer,
    fact_structure_validator=fact_structure_validator,
    fact_reference_validator=fact_reference_validator,
    fact_transformer=fact_transformer,
    version_manager=version_manager,
    storage=model_store,
)

reference_data_store = ReferenceDataStore()

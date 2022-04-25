import asyncio
from pathlib import Path

from lunch.managers.model_manager import ModelManager
from lunch.managers.version_manager import VersionManager
from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_reference_validator import DimensionReferenceValidator
from lunch.model.dimension.dimension_structure_validator import DimensionStructureValidator
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.storage.transformers.dimension_index_transformer import DimensionIndexTransformer
from lunch.storage.cache.null_model_cache import NullModelCache
from lunch.storage.cache.null_version_cache import NullVersionCache
from lunch.storage.model_store import ModelStore
from lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from lunch.storage.persistence.local_file_version_persistor import LocalFileVersionPersistor
from lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from lunch.storage.transformers.versions_transformer import VersionsTransformer
from lunch.storage.version_store import VersionStore


async def main():

    # Validators, Transformers
    version_transformer = VersionsTransformer()
    dimension_transformer = DimensionTransformer()
    dimension_index_transformer = DimensionIndexTransformer()
    dimension_comparer = DimensionComparer()
    dimension_structure_validator = DimensionStructureValidator()
    dimension_reference_validator = DimensionReferenceValidator()

    # Persistence
    version_persistor = LocalFileVersionPersistor(
        directory=Path("/home/treloarja/PycharmProjects/lunch/example_output/dimension")
    )
    model_persistor = LocalFileModelPersistor(
        directory=Path(
            "/home/treloarja/PycharmProjects/lunch/example_output/dimension/model"
        )
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
    model_store = ModelStore(dimension_comparer=dimension_comparer,
                             dimension_transformer=dimension_transformer,
                             dimension_index_transformer=dimension_index_transformer,
                             fact_transformer=None,
                             serializer=model_serializer,
                             cache=model_cache)

    # Managers
    version_manager = VersionManager(storage=version_store)
    model_manager = ModelManager(
        dimension_structure_validator=dimension_structure_validator,
        dimension_comparer=dimension_comparer,
        dimension_reference_validator=dimension_reference_validator,
        dimension_transformer=dimension_transformer,
        fact_comparer=None,
        fact_structure_validator=None,
        fact_reference_validator=None,
        fact_transformer=None,
        version_manager=version_manager,
        storage=model_store,
    )

    my_dim = {"name": "MyDim", "thing": "thing1"}
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[my_dim], facts=[], read_version=read_version, write_version=write_version
            )

    your_dim = {"name": "YourDim", "thing": "thing2"}
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[your_dim], facts=[], read_version=read_version, write_version=write_version
            )

# And run it
asyncio.run(main())

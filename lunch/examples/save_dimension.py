import asyncio
import os
from pathlib import Path

from lunch.managers.model_manager import ModelManager
from lunch.managers.version_manager import VersionManager
from lunch.model.dimension.comparer import DimensionComparer
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.dimension.reference_validator import (
    ReferenceValidator as DimensionReferenceValidator,
)
from lunch.model.dimension.structure_validator import (
    StructureValidator as DimensionStructureValidator,
)
from lunch.storage.cache.null_model_cache import NullModelCache
from lunch.storage.cache.null_version_cache import NullVersionCache
from lunch.storage.model_store import ModelStore
from lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from lunch.storage.persistence.local_file_version_persistor import (
    LocalFileVersionPersistor,
)
from lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from lunch.storage.transformers.versions_transformer import VersionsTransformer
from lunch.storage.version_store import VersionStore


async def main():

    # Validators, Transformers
    version_transformer = VersionsTransformer()
    dimension_transformer = DimensionTransformer()
    dimension_comparer = DimensionComparer()
    dimension_structure_validator = DimensionStructureValidator()
    dimension_reference_validator = DimensionReferenceValidator()

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
    model_store = ModelStore(serializer=model_serializer, cache=model_cache)

    # Managers
    version_manager = VersionManager(storage=version_store)
    model_manager = ModelManager(
        storage=model_store,
        dimension_comparer=dimension_comparer,
        dimension_structure_validator=dimension_structure_validator,
        version_manager=version_manager,
    )

    my_dim = {"name": "MyDim", "thing": "thing"}
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_dimension(
                my_dim, read_version=read_version, write_version=write_version
            )


# And run it
asyncio.run(main())

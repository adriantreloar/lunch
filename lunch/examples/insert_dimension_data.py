from pathlib import Path

import asyncio

from lunch.examples.setup_managers import model_manager, version_manager
from lunch.managers.reference_data_manager import ReferenceDataManager
from lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from lunch.import_engine.dimension_dataframe_merger import DimensionDataFrameMerger
from lunch.storage.reference_data_store import ReferenceDataStore
from lunch.storage.dimension_data_store import DimensionDataStore
from lunch.storage.persistence.local_file_columnar_dimension_data_persistor import LocalFileColumnarDimensionDataPersistor
from lunch.storage.persistence.local_file_reference_data_persistor import LocalFileReferenceDataPersistor # index etc.
from lunch.storage.cache.null_dimension_data_cache import NullDimensionDataCache
from lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from lunch.storage.serialization.columnar_dimension_data_serializer import ColumnarDimensionDataSerializer
from lunch.storage.serialization.yaml_reference_data_serializer import YamlReferenceDataSerializer # For indexes

import pandas as pd

async def main():

    d_test = {
        "name": "Test",
        "attributes": [{"name": "foo"}, {"name": "bar"}, {"name": "baz"}, ],
        "key": ["foo", ],
    }

    data = [{"foo": "a", "bar": 1, "baz": 10, },
            {"foo": "b", "bar": 2, "baz": 20, },
            {"foo": "c", "bar": 3, "baz": 30, }, ]

    df_data = pd.DataFrame(data=data)

    dimension_import_optimiser = DimensionImportOptimiser()
    dimension_dataframe_merger = DimensionDataFrameMerger()

    dimension_data_persistor = LocalFileColumnarDimensionDataPersistor(
        directory=Path("/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension")
    )
    dimension_data_cache = NullDimensionDataCache()
    dimension_serializer = ColumnarDimensionDataSerializer(persistor=dimension_data_persistor)

    dimension_storage = DimensionDataStore(serializer=dimension_serializer, cache=dimension_data_cache)

    reference_data_persistor = LocalFileReferenceDataPersistor(
        directory=Path("/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension")
    )
    reference_data_cache = NullReferenceDataCache()
    reference_data_serializer = YamlReferenceDataSerializer(persistor=reference_data_persistor)

    # Dimensions and Hierarchies deserve special structures, but they also need an overall indexer
    # so that we can check whether Dimensional Data, Hierarchical Data or both have changed
    # for a given version
    reference_storage = ReferenceDataStore(dimension_store = dimension_storage, serializer=reference_data_serializer, cache=reference_data_cache)

    reference_data_manager = ReferenceDataManager(model_manager=model_manager,
                                                  dimension_import_optimiser=dimension_import_optimiser,
                                                  dimension_dataframe_merger=dimension_dataframe_merger,
                                                  reference_storage=reference_storage)

    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(read_version=read_version) as write_version:
            await model_manager.update_model(
                dimensions=[d_test],
                facts=[],
                read_version=read_version,
                write_version=write_version,
            )

    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(read_version=read_version) as write_version:
            await reference_data_manager.update_dimension_from_dataframe(name="d_test",
                                                                         data=df_data,
                                                                         read_version=read_version,
                                                                         write_version=write_version)


# And run it
asyncio.run(main())

import asyncio
from pathlib import Path

import pandas as pd

from lunch.examples.setup_managers import model_manager, version_manager
from lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from lunch.import_engine.dimension_import_planner import DimensionImportPlanner
from lunch.managers.reference_data_manager import ReferenceDataManager
from lunch.storage.cache.null_dimension_data_cache import NullDimensionDataCache
from lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from lunch.storage.dimension_data_store import DimensionDataStore
from lunch.storage.persistence.local_file_columnar_dimension_data_persistor import (
    LocalFileColumnarDimensionDataPersistor,
)
from lunch.storage.persistence.local_file_reference_data_persistor import (
    LocalFileReferenceDataPersistor,
)  # index etc.
from lunch.storage.reference_data_store import ReferenceDataStore
from lunch.storage.serialization.columnar_dimension_data_serializer import (
    ColumnarDimensionDataSerializer,
)
from lunch.storage.serialization.yaml_reference_data_serializer import (
    YamlReferenceDataSerializer,
)  # For indexes

from lunch.examples.save_dimension import save_dimension

async def insert_dimension_data():

    # Create d_test
    await save_dimension()

    data = [
        {
            "foo": "a",
            "bar": 1,
            "baz": 10,
        },
        {
            "foo": "b",
            "bar": 2,
            "baz": 20,
        },
        {
            "foo": "c",
            "bar": 3,
            "baz": 30,
        },
    ]

    df_data = pd.DataFrame(data=data)

    dimension_data_persistor = LocalFileColumnarDimensionDataPersistor(
        directory=Path(
            "/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension"
        )
    )
    dimension_data_cache = NullDimensionDataCache()
    dimension_serializer = ColumnarDimensionDataSerializer(
        persistor=dimension_data_persistor
    )

    # TODO - this DimensionDataStore is redundant, we should be pointing stuff at the ReferenceDataStore
    dimension_data_storage = DimensionDataStore(
        serializer=dimension_serializer, cache=dimension_data_cache
    )

    reference_data_persistor = LocalFileReferenceDataPersistor(
        directory=Path(
            "/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension"
        )
    )
    reference_data_cache = NullReferenceDataCache()
    reference_data_serializer = YamlReferenceDataSerializer(
        persistor=reference_data_persistor
    )

    dimension_import_planner = DimensionImportPlanner()
    dimension_import_optimiser = DimensionImportOptimiser(
        dimension_import_planner=dimension_import_planner,
        dimension_data_store=dimension_data_storage,
        model_manager=model_manager,
    )
    dimension_import_enactor = DimensionImportEnactor()

    # Dimensions and Hierarchies deserve special structures, but they also need an overall indexer
    # so that we can check whether Dimensional Data, Hierarchical Data or both have changed
    # for a given version
    reference_storage = ReferenceDataStore(
        serializer=reference_data_serializer, cache=reference_data_cache
    )

    reference_data_manager = ReferenceDataManager(
        model_manager=model_manager,
        reference_data_store=reference_storage,
        dimension_data_store=dimension_data_storage,
        dimension_import_optimiser=dimension_import_optimiser,
        dimension_import_enactor=dimension_import_enactor,
    )


    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(
            read_version=read_version
        ) as write_version:

            # This is also done in reference_data_manager.update_dimension_from_dataframe()
            # I did it again here just for show
            plan = await dimension_import_optimiser.create_dataframe_import_plan(
                dimension_name="Test",
                data=df_data,
                read_version=read_version,
                write_version=write_version,
            )

            # TODO match this plan with what _enact_plan needs
            print(plan)
            print()
            print(df_data)
            print()

            await reference_data_manager.update_dimension_from_dataframe(
                name="Test",
                data=df_data,
                read_version=read_version,
                write_version=write_version,
            )


# And run it
if __name__ == "__main__":
    asyncio.run(insert_dimension_data())

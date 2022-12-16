import asyncio
from pathlib import Path

import pandas as pd

from src.lunch.examples.setup_managers import model_manager, version_manager

from src.lunch.examples.insert_dimension_data import insert_dimension_data
from src.lunch.examples.save_fact import save_fact

from src.lunch.storage.persistence.local_file_columnar_fact_data_persistor import (
    LocalFileColumnarFactDataPersistor,
)
from src.lunch.storage.cache.null_fact_data_cache import NullFactDataCache
from src.lunch.storage.serialization.columnar_fact_data_serializer import (
    ColumnarFactDataSerializer,
)
from src.lunch.storage.fact_data_store import FactDataStore
from src.lunch.import_engine.fact_append_planner import FactAppendPlanner
from src.lunch.import_engine.fact_import_optimiser import FactImportOptimiser
from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.managers.cube_data_manager import CubeDataManager


async def insert_fact_data():

    await insert_dimension_data()
    await save_fact()

    # TODO save_fact not actually creating the fact table...

    # TODO, change insert_dimension_data to insert some of the other dimensions

    # Create some fact data

    # append it to f_sales


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

    fact_data_persistor = LocalFileColumnarFactDataPersistor(
        directory=Path(
            "../../../example_output/fact"
        )
    )
    fact_data_cache = NullFactDataCache()
    fact_data_serializer = ColumnarFactDataSerializer(
        persistor=fact_data_persistor
    )

    fact_data_storage = FactDataStore(
        serializer=fact_data_serializer, cache=fact_data_cache
    )

    fact_append_planner = FactAppendPlanner()
    fact_import_optimiser = FactImportOptimiser(
        fact_append_planner=fact_append_planner,
        fact_data_store=fact_data_storage,
        model_manager=model_manager,
    )
    fact_import_enactor = FactImportEnactor()

    cube_data_manager = CubeDataManager(
        model_manager=model_manager,
        fact_data_store=fact_data_storage,
        fact_import_optimiser=fact_import_optimiser,
        fact_import_enactor=fact_import_enactor,
    )


    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(
            read_version=read_version
        ) as write_version:

            # This is also done in reference_data_manager.update_dimension_from_dataframe()
            # I did it again here just for show
            plan = await fact_import_optimiser.create_dataframe_append_plan(
                fact_name="Sales",
                data=df_data,
                read_version=read_version,
                write_version=write_version,
            )

            # TODO match this plan with what _enact_plan needs
            print(plan)
            print()
            print(df_data)
            print()

            await cube_data_manager.append_fact_from_dataframe(
                name="Sales",
                data=df_data,
                read_version=read_version,
                write_version=write_version,
            )


# And run it
if __name__ == "__main__":
    asyncio.run(insert_fact_data())
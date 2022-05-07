import asyncio

from lunch.examples.setup_managers import model_manager, version_manager
from lunch.managers.reference_data_manager import ReferenceDataManager
from lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from lunch.import_engine.dimension_dataframe_merger import DimensionDataFrameMerger

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
    dimension_storage =
    reference_data_manager = ReferenceDataManager(model_manager=model_manager,
                                                  dimension_import_optimiser=dimension_import_optimiser,
                                                  dimension_dataframe_merger=dimension_dataframe_merger,
                                                  dimension_storage=dimension_storage)

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

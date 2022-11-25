import asyncio

from lunch.examples.setup_managers import model_manager, version_manager

from lunch.examples.insert_dimension_data import insert_dimension_data
from lunch.examples.save_fact import save_fact

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
            "/home/treloarja/PycharmProjects/lunch/example_output/fact"
        )
    )
    fact_data_cache = NullFactDataCache()
    fact_data_serializer = ColumnarFactDataSerializer(
        persistor=fact_data_persistor
    )

    fact_data_storage = FactDataStore(
        serializer=fact_data_serializer, cache=fact_data_cache
    )

    fact_import_planner = FactImportPlanner()
    fact_import_optimiser = FactImportOptimiser(
        fact_import_planner=fact_import_planner,
        fact_data_store=fact_data_storage,
        model_manager=model_manager,
    )
    fact_import_enactor = FactImportEnactor()



    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(
            read_version=read_version
        ) as write_version:

            # This is also done in reference_data_manager.update_dimension_from_dataframe()
            # I did it again here just for show
            plan = await fact_import_optimiser.create_fact_import_plan(
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

            await fact_data_manager.append_fact_from_dataframe(
                name="Sales",
                data=df_data,
                read_version=read_version,
                write_version=write_version,
            )


# And run it
if __name__ == "__main__":
    asyncio.run(insert_fact_data())
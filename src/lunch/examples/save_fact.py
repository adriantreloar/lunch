import asyncio

from src.lunch.examples.setup_managers import model_manager, version_manager

from src.lunch.examples.save_dimension import save_dimension

from src.lunch.model.fact import Fact, FactStorage

async def save_fact():

    await save_dimension()


    #d_department = {"name": "Department", "attributes": [{"name": "thing1"}]}
    #d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}

    f_sales = Fact(
        name="Sales",
        dimensions=[{"name": "Department", "column_id": 0, "view_order": 0, "dimension_name": "Department", "dimension_id": 0},
                    {"name": "Time", "column_id": 0, "view_order": 0, "dimension_name": "Time", "dimension_id": 0}
                    ],
        measures=[{"name": "sales", "measure_id": 1, "type": "decimal", "precision": 2}],
        storage=FactStorage(index_columns=[1],
                            data_columns=[2, 0]
                            )
        )

    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:

            await model_manager.update_model(
                dimensions=[],
                facts=[f_sales],
                read_version=read_version,
                write_version=write_version,
            )


# And run it
if __name__ == "__main__":
    asyncio.run(save_fact())

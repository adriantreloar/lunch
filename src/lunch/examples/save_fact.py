import asyncio

from src.lunch.examples.setup_managers import model_manager, version_manager

from src.lunch.examples.save_dimension import save_dimension

async def save_fact():

    await save_dimension()
    #d_department = {"name": "Department", "attributes": [{"name": "thing1"}]}
    #d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}

    f_sales = {
        "name": "Sales",
        "dimensions": ["Department", "Time"],
        "measures": [{"name": "sales", "type": "decimal", "precision": "2"}],
    }

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

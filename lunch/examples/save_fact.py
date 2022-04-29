import asyncio

from lunch.examples.setup_managers import model_manager, version_manager


async def main():

    d_department = {"name": "Department", "attributes": [{"name": "thing1"}]}
    d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}

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
                dimensions=[d_department, d_time],
                facts=[f_sales],
                read_version=read_version,
                write_version=write_version,
            )


# And run it
asyncio.run(main())

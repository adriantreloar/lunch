import asyncio

from src.lunch.examples.setup_managers import model_manager, version_manager


async def save_dimension():
    d_department = {"name": "Department", "attributes": [{"name": "thing1"}]}
    d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}
    d_test = {
        "name": "Test",
        "attributes": [
            {"name": "foo"},
            {"name": "bar"},
            {"name": "baz"},
        ],
        "key": [
            "foo",
        ],
    }

    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[d_test],
                facts=[],
                read_version=read_version,
                write_version=write_version,
            )

    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[d_department, d_time],
                facts=[],
                read_version=read_version,
                write_version=write_version,
            )

# And run it
if __name__ == "__main__":
    asyncio.run(save_dimension())

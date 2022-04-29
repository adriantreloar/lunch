import asyncio
from lunch.examples.setup_managers import version_manager, model_manager


async def main():

    my_dim = {"name": "MyDim", "attributes": [{"name":"thing1"}]}
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[my_dim], facts=[], read_version=read_version, write_version=write_version
            )

    your_dim = {"name": "YourDim", "attributes": [{"name":"thing1"}]}
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(
            read_version=read_version
        ) as write_version:
            await model_manager.update_model(
                dimensions=[your_dim], facts=[], read_version=read_version, write_version=write_version
            )

# And run it
asyncio.run(main())

import asyncio

from lunch.examples.setup_managers import model_manager, version_manager


async def main():

    d_department = {
        "name": "Department",
        "attributes": [{"name": "thing1"}, {"name": "thing2"}],
    }
    d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}

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

    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(
            read_version=read_version
        ) as write_version:
            # At time of writing we are doing a fairly raw dimension data put()
            # No key checking, no update capability, no parallel writes for big data, no caching
            # Just put the data into storage
            # Note - no manager here, since the manager's job will be harder,
            # since we'll be connecting rawish interfaces for files with each other
            # in the most performant way possible, rather than using a nice abstract API
            await reference_data_store.store_dimension_stats(
                dimension_id=1,
                dimension_length=3,
                read_version=read_version,
                write_version=write_version,
            )
            await reference_data_store.store_dimension_attribute(
                dimension_id=1,
                attribute_id=1,
                attribute_iterator=["foo", "bar", "baz"],
                read_version=read_version,
                write_version=write_version,
            )
            await reference_data_store.store_dimension_attribute(
                dimension_id=1,
                attribute_id=2,
                attribute_iterator=["foooey", "barrey", "bazzey"],
                read_version=read_version,
                write_version=write_version,
            )


# And run it
asyncio.run(main())

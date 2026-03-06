import asyncio
from pathlib import Path

from src.lunch.examples.setup_managers import model_manager, version_manager
from src.lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from src.lunch.storage.persistence.local_file_reference_data_persistor import (  # index etc.
    LocalFileReferenceDataPersistor,
)
from src.lunch.storage.reference_data_store import ReferenceDataStore
from src.lunch.storage.serialization.yaml_reference_data_serializer import (  # For indexes
    YamlReferenceDataSerializer,
)

_EXAMPLE_OUTPUT = Path(__file__).resolve().parents[3] / "example_output"


async def main():
    # Demonstrates the end-to-end flow for defining dimensions in the star schema and
    # writing dimension member data (reference data) to disk under MVCC version control.
    # Two separate versioned transactions are used: one to update the model (schema),
    # and one to store the actual attribute values for a dimension.

    # Wire up the storage stack for reference (dimension member) data:
    # persistor → serializer → store, with a no-op cache (caching disabled for this example).
    reference_data_persistor = LocalFileReferenceDataPersistor(directory=_EXAMPLE_OUTPUT / "reference" / "dimension")
    reference_data_cache = NullReferenceDataCache()
    reference_data_serializer = YamlReferenceDataSerializer(persistor=reference_data_persistor)

    # Dimensions and Hierarchies deserve special structures, but they also need an overall indexer
    # so that we can check whether Dimensional Data, Hierarchical Data or both have changed
    # for a given version
    reference_data_store = ReferenceDataStore(serializer=reference_data_serializer, cache=reference_data_cache)

    # Define two dimensions as raw dicts; these will be written into the star schema model.
    d_department = {
        "name": "Department",
        "attributes": [{"name": "thing1"}, {"name": "thing2"}],
    }
    d_time = {"name": "Time", "attributes": [{"name": "thing1"}]}

    # Open a read/write version pair and update the star schema model with the two dimensions.
    # This creates a new model_version that records the dimension definitions.
    async with version_manager.read_version() as read_version:
        async with version_manager.write_model_version(read_version=read_version) as write_version:
            await model_manager.update_model(
                dimensions=[d_department, d_time],
                facts=[],
                read_version=read_version,
                write_version=write_version,
            )

    # Open a fresh read/write version pair and write the actual dimension member data.
    # This advances reference_data_version independently of model_version.
    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(read_version=read_version) as write_version:
            # At time of writing we are doing a fairly raw dimension data put()
            # No key checking, no update capability, no parallel writes for big data, no caching
            # Just put the data into storage
            # Note - no manager here, since the manager's job will be harder,
            # since we'll be connecting rawish interfaces for files with each other
            # in the most performant way possible, rather than using a nice abstract API

            # Store metadata (member count) for dimension 1 so readers know its size.
            await reference_data_store.store_dimension_stats(
                dimension_id=1,
                dimension_length=3,
                read_version=read_version,
                write_version=write_version,
            )
            # Write the first attribute column (attribute_id=1) for dimension 1.
            await reference_data_store.store_dimension_attribute(
                dimension_id=1,
                attribute_id=1,
                attribute_iterator=["foo", "bar", "baz"],
                read_version=read_version,
                write_version=write_version,
            )
            # Write the second attribute column (attribute_id=2) for dimension 1.
            await reference_data_store.store_dimension_attribute(
                dimension_id=1,
                attribute_id=2,
                attribute_iterator=["foooey", "barrey", "bazzey"],
                read_version=read_version,
                write_version=write_version,
            )


# And run it
asyncio.run(main())

import pyarrow as pa
import pyarrow.flight
import json
import asyncio

from src.lunch.mvcc.version import version_to_dict

from src.lunch.examples.setup_managers import model_manager, version_manager
from src.lunch.examples.insert_dimension_data import insert_dimension_data

async def translate_dimension_example():

    await insert_dimension_data()

    client = pa.flight.connect("grpc://0.0.0.0:8817")

    upload_descriptor = pa.flight.FlightDescriptor.for_path("streamed.parquet")

    call_options = pa.flight.FlightCallOptions(timeout=None, headers=[("foo".encode("utf8"), "bar".encode("utf8"))])

    async with version_manager.read_version() as read_version:
        command_dict = {"command":"dimension_lookup",
                        "parameters":
                            {"version": version_to_dict(read_version),
                             "dimension_id": 1,
                             "attribute_id": 'foo'
                             }
                        }
        command = json.dumps(command_dict).encode("utf8")
        print(command_dict)
        print()

        upload_descriptor = pa.flight.FlightDescriptor.for_command(command)

        #FlightDescriptor_descriptor
        #FlightCallOptions_options
        writer, reader = client.do_exchange(descriptor=upload_descriptor, options=call_options)
        writer.begin(schema=pa.schema([pa.field('baz', pa.string())]))
        for n in range(2):
            batch = pa.record_batch([
                pa.array(['a\n','b\n','c\n','c\n','b\n','a\n']),
            ], names=["baz"])
            writer.write_batch(batch)
            return_chunk = reader.read_chunk()
            print(pa.Table.from_batches([return_chunk.data]))
        writer.close()

# And run it
if __name__ == "__main__":
    asyncio.run(translate_dimension_example())

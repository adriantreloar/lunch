import datetime

import pyarrow as pa
import pyarrow.flight
import json
import asyncio
import concurrent.futures
from threading import Thread

from src.lunch.mvcc.version import version_to_dict

from src.lunch.examples.setup_managers import model_manager, version_manager
from src.lunch.examples.insert_dimension_data import insert_dimension_data

#def start_worker(loop):
#    """Switch to new event loop and run forever"""
#    asyncio.set_event_loop(loop)
#    loop.run_forever()

async def translate_dimension_example():

    await insert_dimension_data()

    start_time = datetime.datetime.now()
    client = pa.flight.connect("grpc://0.0.0.0:8817")

    call_options = pa.flight.FlightCallOptions(timeout=None, headers=[("foo".encode("utf8"), "bar".encode("utf8"))])


    async with version_manager.read_version() as read_version:
        command_dict = {"command": "dimension_lookup",
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

        # Writer thread

        # TODO return a future from here, check the future OK at end
        # TODO use a thread pool, preferable created in server startup
        worker = Thread(target=_write_batches, args=(writer,))
        ## Start the thread
        worker.start()

        for m, return_chunk in enumerate(reader):
            #print(f"... {m} read chunk {return_chunk}", datetime.datetime.utcnow())
            tab = pa.Table.from_batches([return_chunk.data])
            #print("received rows:", tab.num_rows, ", bytes: ", tab.nbytes, datetime.datetime.utcnow())

        print("joining worker thread", datetime.datetime.utcnow())
        worker.join()

        print(f"TIME {datetime.datetime.utcnow()-start_time}")
        print(f"FIN", datetime.datetime.utcnow())

async def _async_write_batches(writer):
    return _write_batches(writer)

def _write_batches(writer):
    print(f"beginning writer...", datetime.datetime.utcnow())
    writer.begin(schema=pa.schema([pa.field('baz', pa.string())]))
    total_rows = 0
    total_bytes = 0
    TUNING_FACTOR = 2
    for n in range(10):
        batch = pa.record_batch([
            pa.array(['a', 'b', 'c', 'c'] * int(1024 * 128)),
        ], names=["baz"])
        print("sending rows:", batch.num_rows, ", bytes: ", batch.nbytes)
        total_rows += batch.num_rows
        total_bytes += batch.nbytes
        print(f"translating {n}...", datetime.datetime.utcnow())
        writer.write_batch(batch)
        print(f"...batch {n} written...", datetime.datetime.utcnow())
    print(f"closing writer...", datetime.datetime.utcnow())
    writer.close()
    print(f"... writer closed", datetime.datetime.utcnow())
    print(f"TOTAL sent {total_rows} rows, {total_bytes} bytes")

# And run it
if __name__ == "__main__":
    asyncio.run(translate_dimension_example())

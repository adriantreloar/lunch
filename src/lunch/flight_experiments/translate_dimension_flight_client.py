import datetime

import pyarrow as pa
import pyarrow.flight
import json
import asyncio
import concurrent.futures as futures
from threading import Thread
import time

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

        print(writer)
        print(reader)

        # Writer thread
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            write_future = executor.submit(_write_batches, writer, pa.schema([pa.field('baz', pa.string())]))

            # TODO return a future from here, check the future OK at end
            # TODO use a thread pool, preferable created in server startup
            #worker = Thread(target=_write_batches, args=(writer, pa.schema([pa.field('baz', pa.string())])))
            #worker.start()


            #while True:
            #    return_chunk = reader.read_chunk()
            def read_it_all(reader):
                total_rows = 0
                total_bytes = 0
                for m, return_chunk in enumerate(reader.to_reader()):
                    print(f"... {m} read chunk {return_chunk}", datetime.datetime.utcnow())
                    tab = pa.Table.from_batches([return_chunk])
                    total_rows += tab.num_rows
                    total_bytes += tab.nbytes
                    print("received rows:", tab.num_rows, ", bytes: ", tab.nbytes, datetime.datetime.utcnow())
                    print("received total:", total_rows, ", bytes: ", total_bytes, datetime.datetime.utcnow())

                print(f"READ ROWS {total_rows}")
                print(f"READ BYTES {total_bytes}")
                return total_rows, total_bytes


            read_future = executor.submit(read_it_all, reader)

            #print("sleeping")
            #time.sleep(3)
            #await asyncio.sleep(2)
            print("joining worker threads", datetime.datetime.utcnow())
            #worker.join()
            print(write_future.result(timeout=10))

            print(read_future.result(timeout=10))
            print("joined worker thread", datetime.datetime.utcnow())

            writer.close()

        print(f"READ  TIME {datetime.datetime.utcnow()-start_time}")
        print(f"FIN", datetime.datetime.utcnow())


def _write_batches(writer, schema):
    print(f"beginning writer...", datetime.datetime.utcnow())
    writer.begin(schema=schema)
    start_time = datetime.datetime.utcnow()
    total_rows = 0
    total_bytes = 0
    for n in range(5):
        batch = pa.record_batch([
            pa.array(['a', 'b', 'c', 'c'] * int(32768*8)),
        ], schema=schema)
        total_rows += batch.num_rows
        total_bytes += batch.nbytes
        writer.write_batch(batch)

    writer.done_writing()
    print(f"WRITE ROWS {total_rows}")
    print(f"WRITE BYTES {total_bytes}")
    print(f"WRITE  TIME {datetime.datetime.utcnow() - start_time}")

    return total_rows

# And run it
if __name__ == "__main__":
    asyncio.run(translate_dimension_example())

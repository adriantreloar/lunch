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

async def sort_keys_example():

    #await insert_dimension_data()

    start_time = datetime.datetime.now()
    client = pa.flight.connect("grpc://0.0.0.0:8818")

    call_options = pa.flight.FlightCallOptions(timeout=None, headers=[("foo".encode("utf8"), "bar".encode("utf8"))])


    async with version_manager.read_version() as read_version:
        command_dict = {"command": "sort_and_group_fact_partition_keys",
                        "parameters":
                            {"key": []}
                        }
        command = json.dumps(command_dict).encode("utf8")
        print(command_dict)
        print()

        upload_descriptor = pa.flight.FlightDescriptor.for_command(command)
        writer, reader = client.do_exchange(descriptor=upload_descriptor, options=call_options)

        print(writer)
        print(reader)

        # Writer thread
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            print("submitting write thread")
            write_future = executor.submit(_write_batches, writer, pa.schema([pa.field('k0', pa.int32()),
                                                                              pa.field('k1', pa.int32())]))
            print("submitted write thread")

            def read_it_all(reader):
                total_rows = 0
                total_bytes = 0

                print(f"about to loop reader")
                for m, return_chunk in enumerate(reader): #.to_reader()):
                    print(f"... {m} read chunk {return_chunk}", datetime.datetime.utcnow())
                    tab = pa.Table.from_batches([return_chunk.data])
                    total_rows += tab.num_rows
                    total_bytes += tab.nbytes
                    partition_slices = json.loads(return_chunk.app_metadata.to_pybytes().decode("utf8"))
                    print("returned metadata", partition_slices)
                    print("received rows:", tab.num_rows, ", bytes: ", tab.nbytes, datetime.datetime.utcnow())
                    print("received total:", total_rows, ", bytes: ", total_bytes, datetime.datetime.utcnow())

                print(f"READ ROWS {total_rows}")
                print(f"READ BYTES {total_bytes}")
                return total_rows, total_bytes

            print("submitting read thread")
            read_future = executor.submit(read_it_all, reader)
            print("submitted read thread")

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
        MULTIPLIER = int(32768 * 8)
        batch = pa.record_batch([
            pa.array([2, 2, n, n] * MULTIPLIER),
            pa.array([1, 1, 2, 1] * MULTIPLIER),
        ], schema=schema)
        total_rows += batch.num_rows
        total_bytes += batch.nbytes
        print(f"writing batch rows {batch.num_rows}")
        writer.write_batch(batch)
        print(f"written batch")

    writer.done_writing()
    print(f"WRITE ROWS {total_rows}")
    print(f"WRITE BYTES {total_bytes}")
    print(f"WRITE  TIME {datetime.datetime.utcnow() - start_time}")

    return total_rows

# And run it
if __name__ == "__main__":
    asyncio.run(sort_keys_example())

import pyarrow as pa
import pyarrow.flight

client = pa.flight.connect("grpc://0.0.0.0:8815")

# Upload a new dataset
NUM_BATCHES = 1024
ROWS_PER_BATCH = 4096
upload_descriptor = pa.flight.FlightDescriptor.for_path("streamed.parquet")

batch = pa.record_batch([
    pa.array(range(ROWS_PER_BATCH)),
], names=["ints"])
# writer, _ = client.do_put(upload_descriptor, batch.schema)
# with writer:
#     for _ in range(NUM_BATCHES):
#         writer.write_batch(batch)


# Read content of the dataset
# flight = client.get_flight_info(upload_descriptor)
# reader = client.do_get(flight.endpoints[0].ticket)
# total_rows = 0
# for chunk in reader:
#     total_rows += chunk.data.num_rows
# print("Got", total_rows, "rows total, expected", NUM_BATCHES * ROWS_PER_BATCH)

call_options=pa.flight.FlightCallOptions(timeout=None, headers=[("foo".encode("utf8"), "bar".encode("utf8"))])

#batch.schema

#write_options: pyarrow.ipc.IpcWriteOptions
#headers: List[Tuple[str, str]], optional
#A list of arbitrary headers as key, value tuples
#read_options: pyarrow.ipc.IpcReadOptions

upload_descriptor = pa.flight.FlightDescriptor.for_command("foo guffaw".encode("utf8"))

#FlightDescriptor_descriptor
#FlightCallOptions_options
writer, reader = client.do_exchange(descriptor=upload_descriptor, options=call_options)
writer.begin(schema=batch.schema)
for n in range(2):
    batch = pa.record_batch([
        pa.array(range(n+ROWS_PER_BATCH)),
    ], names=["ints"])
    writer.write_batch(batch)
    return_chunk = reader.read_chunk()
    print(pa.Table.from_batches([return_chunk.data]))
writer.close()


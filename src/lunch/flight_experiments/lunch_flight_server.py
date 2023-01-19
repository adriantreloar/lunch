import logging
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import pyarrow.csv

import numpy as np

import asyncio
from pathlib import Path
import json



from src.lunch.mvcc.version import Version, version_from_dict
from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from src.lunch.import_engine.dimension_import_planner import DimensionImportPlanner
from src.lunch.managers.reference_data_manager import ReferenceDataManager
from src.lunch.storage.cache.null_dimension_data_cache import NullDimensionDataCache
from src.lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.persistence.local_file_columnar_dimension_data_persistor import (
    LocalFileColumnarDimensionDataPersistor,
)
from src.lunch.storage.persistence.local_file_reference_data_persistor import (
    LocalFileReferenceDataPersistor,
)  # index etc.
from src.lunch.storage.reference_data_store import ReferenceDataStore
from src.lunch.storage.serialization.columnar_dimension_data_serializer import (
    ColumnarDimensionDataSerializer,
)
from src.lunch.storage.serialization.yaml_reference_data_serializer import (
    YamlReferenceDataSerializer,
)  # For indexes

from src.lunch.globals.global_state import GlobalState
from src.lunch.managers.model_manager import ModelManager
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import (
    DimensionReferenceValidator,
)
from src.lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator,
)
from src.lunch.model.dimension.dimension_transformer import DimensionTransformer
from src.lunch.model.star_schema import StarSchemaTransformer


from src.lunch.mvcc.versions_transformer import VersionsTransformer
from src.lunch.storage.cache.null_model_cache import NullModelCache
from src.lunch.storage.cache.null_version_cache import NullVersionCache
from src.lunch.storage.model_store import ModelStore
from src.lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from src.lunch.storage.persistence.local_file_version_persistor import (
    LocalFileVersionPersistor,
)
from src.lunch.storage.persistence.local_file_parquet_fact_data_persistor import LocalFileParquetFactDataPersistor
from src.lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from src.lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from src.lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
)
from src.lunch.storage.transformers.fact_model_index_transformer import FactModelIndexTransformer
from src.lunch.model.fact import FactDimensionMetadatum

# Constant Global State
global_state = GlobalState()

# Validators, Transformers
version_transformer = VersionsTransformer()
dimension_transformer = DimensionTransformer()
dimension_index_transformer = DimensionModelIndexTransformer()
dimension_comparer = DimensionComparer()
dimension_structure_validator = DimensionStructureValidator()
dimension_reference_validator = DimensionReferenceValidator()
fact_index_transformer = FactModelIndexTransformer()

log = logging.getLogger()

class LunchFlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8819", **kwargs):

        model_manager, reference_data_manager, dimension_data_storage, fact_data_persistor = self._setup_managers(path=Path("/home/treloarja/PycharmProjects/lunch/example_output"))
        self._model_manager = model_manager
        self._reference_data_manager = reference_data_manager
        self._dimension_data_store = dimension_data_storage
        self._fact_data_persistor = fact_data_persistor
        # super() starts serving immediately, so do set up before calling super()
        super(LunchFlightServer, self).__init__(location, **kwargs)

    def _setup_managers(self, path: Path):

        model_path = path / "model"
        dimension_data_path = path / "reference/dimension"
        fact_data_path = path / "fact"

        # Persistence
        version_persistor = LocalFileVersionPersistor(directory=path)
        model_persistor = LocalFileModelPersistor(directory=model_path)
        dimension_data_persistor = LocalFileColumnarDimensionDataPersistor(directory=dimension_data_path)
        reference_data_persistor = LocalFileReferenceDataPersistor(directory=dimension_data_path)
        fact_data_persistor = LocalFileParquetFactDataPersistor(directory=fact_data_path)

        # Serializers
        version_serializer = YamlVersionSerializer(
            persistor=version_persistor, transformer=version_transformer
        )
        model_serializer = YamlModelSerializer(persistor=model_persistor)
        dimension_serializer = ColumnarDimensionDataSerializer(
            persistor=dimension_data_persistor
        )
        reference_data_serializer = YamlReferenceDataSerializer(
            persistor=reference_data_persistor
        )

        # Caches
        version_cache = NullVersionCache()
        model_cache = NullModelCache()
        dimension_data_cache = NullDimensionDataCache()
        reference_data_cache = NullReferenceDataCache()

        # Storage
        model_store = ModelStore(
            dimension_comparer=dimension_comparer,
            dimension_transformer=dimension_transformer,
            dimension_index_transformer=dimension_index_transformer,
            fact_index_transformer=fact_index_transformer,
            serializer=model_serializer,
            cache=model_cache,
        )
        # TODO - this DimensionDataStore is redundant, we should be pointing stuff at the ReferenceDataStore
        dimension_data_storage = DimensionDataStore(
            serializer=dimension_serializer, cache=dimension_data_cache
        )
        # Dimensions and Hierarchies deserve special structures, but they also need an overall indexer
        # so that we can check whether Dimensional Data, Hierarchical Data or both have changed
        # for a given version
        reference_storage = ReferenceDataStore(
            serializer=reference_data_serializer, cache=reference_data_cache
        )

        # Managers, Plan Optimisation + Plan Enactors
        model_manager = ModelManager(
            dimension_structure_validator=dimension_structure_validator,
            dimension_comparer=dimension_comparer,
            dimension_reference_validator=dimension_reference_validator,
            dimension_transformer=dimension_transformer,
            storage=model_store,
            global_state=global_state,
        )

        dimension_import_planner = DimensionImportPlanner()
        dimension_import_optimiser = DimensionImportOptimiser(
            dimension_import_planner=dimension_import_planner,
            dimension_data_store=dimension_data_storage,
            model_manager=model_manager,
        )
        dimension_import_enactor = DimensionImportEnactor()

        reference_data_manager = ReferenceDataManager(
            reference_data_store=reference_storage,
            dimension_data_store=dimension_data_storage,
            dimension_import_optimiser=dimension_import_optimiser,
            dimension_import_enactor=dimension_import_enactor,
        )

        return model_manager, reference_data_manager, dimension_data_storage, fact_data_persistor


    def _make_flight_info(self, dataset):
        # TODO - create one of these that takes versions and partition keys
        #  and hands out flight info
        dataset_path = self._repo / dataset
        schema = pa.parquet.read_schema(dataset_path)
        metadata = pa.parquet.read_metadata(dataset_path)
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode('utf-8')
        )
        endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
        return pyarrow.flight.FlightInfo(schema,
                                        descriptor,
                                        endpoints,
                                        metadata.num_rows,
                                        metadata.serialized_size)

    def list_flights(self, context, criteria):
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def do_exchange(self, context, descriptor, reader, writer):
        # Read the uploaded data and write it back immediately
        command = json.loads(descriptor.command.decode("utf8"))
        print(command)
        print()

        if command["command"] == "dimension_lookup":
            self._translate_dimension(command, reader, writer)
        elif command["command"] == "sort_and_group_fact_partition_keys":
            self._sort_and_group_fact_partition_keys(command, reader, writer)

        else:
            raise NotImplementedError(command["command"])

    def do_put(self, context, descriptor, reader, writer):
        # Note writer is a metadata writer, allowing us to send metadata back to the client

        command = json.loads(descriptor.command.decode("utf8"))
        print(command)
        print()

        if command["command"] == "import_fact_from_csv":
            self._import_fact_from_csv(command=command, reader=reader, metadata_writer=writer)
        elif command["command"] == "write_fact_data":
            self._write_fact_data_file(command=command, reader=reader, metadata_writer=writer)
        elif command["command"] == "write_fact_partition_index":
            self._write_fact_partition_index(command=command, reader=reader, metadata_writer=writer)
        elif command["command"] == "write_fact_version_index":
            self._write_fact_version_index(command=command, reader=reader, metadata_writer=writer)
        else:
            raise NotImplementedError(command["command"])

    def _import_fact_from_csv(self, command, reader, metadata_writer):

        read_version = command["parameters"]["read_version"]
        write_version = command["parameters"]["write_version"]

        read_version_target_schema = StarSchemaTransformer.from_dict(command["parameters"]["read_version_target_schema"])
        write_version_target_schema = StarSchemaTransformer.from_dict(command["parameters"]["write_version_target_schema"])

        source_schema = command["parameters"]["source_schema"]
        column_mappings = command["parameters"]["column_mappings"]

        csv_file_path = command["parameters"]["csv_file_path"]
        csv_has_headers = command["parameters"]["csv_has_headers"]

        column_names = source_schema["column_names"]
        column_type_names = source_schema["column_types"]
        data_schema = pa.schema([pa.field(n, pa.string() if t == 'object' else pa.from_numpy_dtype(np.dtype(t))) for n, t in zip(column_names, column_type_names)])
        print(data_schema)
        #[pa.field('sort_index', pa.int32())]

        # NEXT: We need to know - what we are translating to what
        # in the example there are 2 translations and a value
        # also there is a measure to broadcast

        # link command['column_mappings'] to 'source_schema' and 'write_version_target_schema'
        # Use the column name e.g. 'department_thing' to send each column to the translator service/function
        # use the target information as parameters in the translator service/function


        # e.g.
        # column_mapping = [{"source": ["department_thing"],
        #                    "target": ["Department", "thing1"]
        #                    },
        #                   {"source": ["thing 2"],
        #                    "target": ["Time", "thing2"],
        #                    },
        #                   {"source": ["sales value"],
        #                    # A measure target puts the original value into 'value'
        #                    # amd the measure id into 'measure dimension'
        #                    "measure target": ["measures", "sales"],
        #                    }
        #                   ]

        # First index the column mapping, so we can do easy lookups.
        # When we iterate over the target schema (to see what needs to be filled in)
        # we will use this lookup to see if we have a mapping, or if we need to default
        # keep the whole of the mapping on the right side of the lookup,
        # so that, as the mapping objects alter over software versions, we don't have to fiddle with this code
        dimension_mappings = {}
        measure_mappings = {}

        print(f"{column_mappings=}")

        for column_mapping in column_mappings:
            dimension_target = column_mapping.get("target")
            if dimension_target is not None:
                dimension_name_on_fact, _ = dimension_target
                dimension_mappings[dimension_name_on_fact] = column_mapping
            else:
                measure_target = column_mapping.get("measure target")
                _, measure_name = measure_target
                measure_mappings[measure_name] = column_mapping

        print(f"{dimension_mappings=}")

        # 'write_version_target_schema':
        # {'fact':
        #   {'name': 'Sales',
        #   'fact_id': 1,
        #   'model_version': 6,
        #   'dimensions': [
        #     {'name': 'Department',
        #     'view_order': 1,
        #     'column_id': 1,
        #     'dimension_name': 'Department',
        #     'dimension_id': 5},
        #     {'name': 'Time',
        #     'view_order': 2,
        #     'column_id': 2,
        #     'dimension_name': 'Time',
        #     'dimension_id': 6}
        #     ],
        #   'measures': [
        #     {'name': 'sales',
        #     'measure_id': 1,
        #     'type': 'decimal',
        #     'precision': 2}],
        #   'storage': {
        #     'index_columns': [1],
        #     'data_columns': [2, 0]}
        #     },
        #   'dimensions': {
        #     5: {'attributes': [
        #          {'id_': 1, 'name': 'thing1'}],
        #          'id_': 5,
        #          'model_version': 5,
        #          'name': 'Department'},
        #     6: {'attributes': [
        #          {'id_': 1, 'name': 'thing2'}],
        #          'id_': 6,
        #          'model_version': 5,
        #          'name': 'Time'
        #          }
        #        }
        #     }

        # All dimensions must be mapped. We can broadcast 0 to the dimension for unmapped
        # We need one instruction for each dimension
        # we need one instruction per mapped measure column
        # we need to know, for each column, whether we are index, or data, and where we sit in the whole shebang

        all_dimension_mappings = {}
        # For each dimension in the fact, decide the mapping type, and extra info about the mapping
        for dim_as_fact_sees_it_ in write_version_target_schema.fact.dimensions:

            dim_as_fact_sees_it: FactDimensionMetadatum = dim_as_fact_sees_it_

            dimension_mapping = dimension_mappings.get(dim_as_fact_sees_it.name)

            if dimension_mapping:
                # There is a column which maps to this dimension

                dimension = write_version_target_schema.dimensions[dim_as_fact_sees_it.dimension_id]
                dimension_mapping["mapping_type"] = "dimension_column_lookup"

                # Fill in the direct information we need to map the dimension
                dimension_name_on_fact, dimension_attribute_name = dimension_mapping["target"]
                assert dimension_name_on_fact == dim_as_fact_sees_it.name
                dimension_mapping["target_dimension"] = dimension
                dimension_mapping["target_dimension_id"] = dimension["id_"]
                for attribute in dimension["attributes"]:
                    if attribute["name"] == dimension_attribute_name:
                        dimension_mapping["target_attribute_id"] = attribute["id_"]
                        break
            else:
                # We need to map a default for this dimension, since we don't have a column mapping
                dimension_mapping = {"mapping_type": "dimension_default"}

            all_dimension_mappings[dim_as_fact_sees_it.dimension_id] = dimension_mapping

        print(f"{all_dimension_mappings=}")

        # For each mapped measure, decide the mapping type (e.g. translate/broadcast),
        # and extra information like the value column and type,

        #  TODO: NEXT



        # NOTE: pyarrow.csv.read_csv will read a single table, but will do it multithreaded
        # ideally we'd optimise to use pyarrow.csv.read_csv if the file size is small enough

        csv_read_options = pa.csv.ReadOptions(use_threads=None,
                                                   block_size=None,
                                                   skip_rows=None,
                                                   column_names= column_names,
                                                   autogenerate_column_names=None,
                                                   encoding='utf8',
                                                   skip_rows_after_names=None)

        csv_parse_options = pyarrow.csv.ParseOptions(delimiter=None,
                                                     quote_char=None,
                                                     double_quote=None,
                                                     escape_char=None,
                                                     newlines_in_values=None,
                                                     ignore_empty_lines=None,
                                                     invalid_row_handler=None
                                                     )

        csv_convert_options = pa.csv.ConvertOptions(check_utf8=None,
                                                    column_types = data_schema,
                                                    null_values=None,
                                                    true_values=None,
                                                    false_values=None,
                                                    decimal_point=None,
                                                    strings_can_be_null=None,
                                                    quoted_strings_can_be_null=None,
                                                    include_columns=None,
                                                    include_missing_columns=None,
                                                    auto_dict_encode=None,
                                                    auto_dict_max_cardinality=None,
                                                    timestamp_parsers=None
                                                    )

        csv_reader = pa.csv.open_csv(csv_file_path,
                                     read_options=csv_read_options,
                                     parse_options=None,
                                     convert_options=None,
                                     memory_pool=None
                                     )


        #write_version_target_schema

        # For starters, read everything in the reader,
        # and return a json binary dumped dictionary with number of rows in the metadatawriter
        for n, batch in enumerate(csv_reader):
            print(n, batch)




    def _write_fact_data_file(self, command, reader, metadata_writer):
        cube_data_version = command["parameters"]["version"].cube_data_version
        fact_id = command["parameters"]["fact_id"]
        partition = command["parameters"]["partition"]

        # TODO - NEXT - read all data in to one big table
        #  Sort the data
        #  only then write it

        # TODO - wrap the following functionality in the serializer

        # Read the uploaded data and write to Parquet incrementally
        with self._fact_data_persistor.open_data_file_write(cube_data_version=cube_data_version,
                                                            fact_id=fact_id,
                                                            partition=partition) as sink:
            with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))

    def _write_fact_partition_index(self, command, reader, metadata_writer):
        cube_data_version = command["parameters"]["version"].cube_data_version
        fact_id = command["parameters"]["fact_id"]

        # Read the uploaded data and write to Parquet incrementally
        with self._fact_data_persistor.open_partition_version_index_file_write(cube_data_version=cube_data_version,
                                                            fact_id=fact_id) as sink:
            with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))

    def _write_fact_version_index(self, command, reader, metadata_writer):
        cube_data_version = command["parameters"]["version"].cube_data_version

        # Read the uploaded data and write to Parquet incrementally
        with self._fact_data_persistor.open_version_index_file_write(cube_data_version=cube_data_version) as sink:
            with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))

    def _sort_and_group_fact_partition_keys(self, command, reader, writer):
        """
        Sort fact partition key data on a batch by batch basis:
        Each batch of keys which is read, will cause the sort index for that batch
        to be written back as data,
        with a list of the partition keys, and the length of each partition key slice as metadata.

        The sort index can be used to sort the fact data.
        The list of partition keys can be used to slice the sorted data into chunks for writing
        to storage.

        :param command:
        :param reader: Reader with batches of columns [key0, key1, ...] as described in the command
        :param writer: Writer with a single column (sort index for each record batch) in the data,
            and a list of dictionaries e.g. [{'count': 20, 'key0:' 1, 'key1': 1, ...},...] in the metadata
            The sort index will give us the index of the data sorted by key0, key1 (as described in command)
            The list of dictionaries will give us the counts we need to slice sorted data,
            and the keys to send them on for further processing
        :return:
        """
        command_parameters = command["parameters"]
        key_ = command_parameters["key"]

        output_schema = pa.schema([pa.field('sort_index', pa.int32())])
        writer.begin(output_schema)
        total_read_rows = 0
        total_write_rows = 0
        for m, chunk in enumerate(reader):

            # print(f"{chunk=}")
            total_read_rows += chunk.data.num_rows

            sort_key_names = [field.name for field in chunk.data.schema]
            sort_keys = [(name, "ascending") for name in sort_key_names]
            print(f"sorting input by", sort_keys)
            sort_index = pa.compute.sort_indices(chunk.data,
                                                 sort_keys=sort_keys,
                                                 null_placement='at_end',
                                                 options=None,
                                                 memory_pool=None)

            keys_sorted = chunk.data.take(sort_index)

            grouped = pa.TableGroupBy(keys_sorted, sort_key_names)
            print(f"{grouped.keys=}")
            grouped.aggregate([(sort_key_names[0], "count")])
            slices = grouped.aggregate([(sort_key_names[0], "count")]).to_pylist()
            print(f"{slices=}")

            slices_as_binary = json.dumps(slices).encode("utf8")

            output_table = pa.record_batch([sort_index], schema=output_schema)
            total_write_rows += output_table.num_rows

            # TODO - I'm not sure the data and metadata will stick together when done this way...
            print(f"writing metadata {m}")
            #writer.write_metadata(slices_as_binary)
            print(f"writing output {m}")
            #writer.write(output_table)
            #for batch in output_table.batches():
            writer.write_with_metadata(output_table, slices_as_binary)
            print(f"output written {m}")
        print(f"Total read {total_read_rows}, Total write {total_write_rows}")
        print("closing")
        writer.close()

    def _translate_dimension(self, command, reader, writer):
        version = version_from_dict(command["parameters"]["version"])
        dimension_id = command["parameters"]["dimension_id"]
        attribute_id = command["parameters"]["attribute_id"]
        coro = self._get_dimension_lookup_table(version=version, dimension_id=dimension_id, attribute_id=attribute_id)
        dimension_lookup_table = asyncio.run(coro)
        # print(dimension_lookup_table)
        output_schema = pa.schema([pa.field('sk', pa.int32())])
        total_read_rows = 0
        total_write_rows = 0
        writer.begin(output_schema)
        for m, chunk in enumerate(reader):
            # print(f"{chunk=}")
            print(f"generating original order for {chunk.data.num_rows=}")
            orig_order = pa.array(range(chunk.data.num_rows), type=pa.int32())
            print(f"building input table")
            input_table = pa.table([chunk.data.column(0), orig_order], names=["nk", "orig_order"])
            total_read_rows += chunk.data.num_rows
            print(f"joining translation")

            input_with_sk = input_table.join(right_table=dimension_lookup_table,
                                             keys=["nk"],
                                             right_keys=["nk"],
                                             join_type='left outer',
                                             left_suffix="l_",
                                             right_suffix="r_",
                                             coalesce_keys=True,
                                             use_threads=True
                                             )
            print(f"sorting output to original sort order")
            # The join has wrecked the original sort order, so sort the data back again
            keys_sorted = input_with_sk["sk"].take(input_with_sk["orig_order"])
            print(f"writing output {m}")
            output_table = pa.table([keys_sorted], schema=output_schema)
            total_write_rows += output_table.num_rows
            writer.write(output_table)
            print(f"output written {m}")
        print(f"Total read {total_read_rows}, Total write {total_write_rows}")
        print("closing writer")
        writer.close()

    def list_actions(self, context):
        return []

    async def _get_dimension_lookup_table(self, version: Version, dimension_id: int, attribute_id: int) -> pa.Table:
        # TODO - generalise this to multiple columns, and pass through the type properly
        # TODO - dimension data manager could have a function to get a lookup table, encapsulating this logic

        # NOTE - column 0 is id column
        columns = await self._dimension_data_store.get_columns(
                                                     read_version=version,
                                                     dimension_id=dimension_id,
                                                     column_types={attribute_id: str},
                                                     filter=None)


        lkp_attribute_array = pa.array(columns[attribute_id], type=pa.string())

        # TODO - Hack - we need to geenrate id's in the model,
        #  not create them on the fly in the translation code!
        id_array = pa.array(range(len(lkp_attribute_array)), type=pa.int32())

        lookup_table = pa.table([lkp_attribute_array, id_array], names=["nk", "sk"])

        return lookup_table

if __name__ == '__main__':

    server = LunchFlightServer()
    server.serve()

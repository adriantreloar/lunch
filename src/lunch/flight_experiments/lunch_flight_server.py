import logging
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet

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
from src.lunch.mvcc.versions_transformer import VersionsTransformer
from src.lunch.storage.cache.null_model_cache import NullModelCache
from src.lunch.storage.cache.null_version_cache import NullVersionCache
from src.lunch.storage.model_store import ModelStore
from src.lunch.storage.persistence.local_file_model_persistor import LocalFileModelPersistor
from src.lunch.storage.persistence.local_file_version_persistor import (
    LocalFileVersionPersistor,
)
from src.lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from src.lunch.storage.serialization.yaml_version_serializer import YamlVersionSerializer
from src.lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
)
from src.lunch.storage.transformers.fact_model_index_transformer import FactModelIndexTransformer

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

    def __init__(self, location="grpc://0.0.0.0:8817", **kwargs):

        model_manager, reference_data_manager, dimension_data_storage = self._setup_managers(path=Path("/home/treloarja/PycharmProjects/lunch/example_output"))
        self._model_manager = model_manager
        self._reference_data_manager = reference_data_manager
        self._dimension_data_store = dimension_data_storage
        # super() starts serving immediately, so do set up before calling super()
        super(LunchFlightServer, self).__init__(location, **kwargs)

    def _setup_managers(self, path: Path):

        model_path = path / "model"
        dimension_data_path = path / "reference/dimension"

        # Persistence
        version_persistor = LocalFileVersionPersistor(directory=path)
        model_persistor = LocalFileModelPersistor(directory=model_path)
        dimension_data_persistor = LocalFileColumnarDimensionDataPersistor(directory=dimension_data_path)
        reference_data_persistor = LocalFileReferenceDataPersistor(directory=dimension_data_path)

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

        return model_manager, reference_data_manager, dimension_data_storage


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

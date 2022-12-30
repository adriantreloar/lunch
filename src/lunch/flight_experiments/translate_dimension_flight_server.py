import logging

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

class TranslateDimensionFlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8817", **kwargs):

        model_manager, reference_data_manager, dimension_data_storage = self._setup_managers(path=Path("/home/treloarja/PycharmProjects/lunch/example_output"))
        self._model_manager = model_manager
        self._reference_data_manager = reference_data_manager
        self._dimension_data_store = dimension_data_storage
        # super() starts serving immediately, so do set up before calling super()
        super(TranslateDimensionFlightServer, self).__init__(location, **kwargs)

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
            version = version_from_dict(command["parameters"]["version"])
            dimension_id = command["parameters"]["dimension_id"]
            attribute_id = command["parameters"]["attribute_id"]

            coro = self._get_dimension_lookup_table(version=version, dimension_id=dimension_id, attribute_id=attribute_id)
            dimension_lookup_table = asyncio.run(coro)
            # print(dimension_lookup_table)
            output_schema = pa.schema([pa.field('sk', pa.int32())])

            writer.begin(output_schema)
            for chunk in reader:
                # print(f"{chunk=}")
                print(f"generating original order for {chunk.data.num_rows=}")
                orig_order = pa.array(range(chunk.data.num_rows), type=pa.int32())
                print(f"building input table")
                input_table = pa.table([chunk.data.column(0), orig_order], names=["nk", "orig_order"])
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
                print(f"sorting output")
                keys_sorted = input_with_sk["sk"].take(input_with_sk["orig_order"])
                # print(f"{keys_sorted=}")
                print(f"writing output")
                writer.write(pa.table([keys_sorted], schema=output_schema))
                print(f"output written")
        else:
            raise NotImplementedError(command["command"])

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

    server = TranslateDimensionFlightServer()
    server.serve()

from pandas import DataFrame

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.fact_append_planner import FactAppendPlanner
from src.lunch.plans.plan import Plan
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.storage.fact_data_store import FactDataStore
from src.lunch.model.star_schema import StarSchema, StarSchemaTransformer
from src.lunch.model.table_metadata import TableMetadata, TableMetadataTransformer

class FactImportOptimiser(Conductor):
    def __init__(
        self,
        fact_append_planner: FactAppendPlanner,
        fact_data_store: FactDataStore,
        model_manager: ModelManager,
    ):
        self._fact_append_planner = fact_append_planner
        #TODO add service that gets processor availablity

    async def create_dataframe_append_plan(
        self,
        read_version_target_model: StarSchema,
        write_version_target_model: StarSchema,
        source_metadata: TableMetadata,
        column_mapping: dict,
        read_version: Version,
        write_version: Version,
    ) -> Plan:
        return await _create_dataframe_append_plan(
            read_version_target_model=read_version_target_model,
            write_version_target_model=write_version_target_model,
            source_metadata=source_metadata,
            column_mapping=column_mapping,
            read_version=read_version,
            write_version=write_version,
            fact_append_planner=self._fact_append_planner,
        )


async def _create_dataframe_append_plan(
    read_version_target_model: StarSchema,
    write_version_target_model: StarSchema,
    source_metadata: TableMetadata,
    column_mapping: dict,
    read_version: Version,
    write_version: Version,
    fact_append_planner: FactAppendPlanner,
) -> Plan:

    #TODO - use service that gets processor availablity, and pass availability onto
    #  fact_append_planner.create_local_dataframe_append_plan


    # TODO - Pass in rules about the capability of the engine - is the engine a grid, or local dask etc.
    #  This will influence the choices the optimiser makes
    # TODO - merge is hardcoded to the first column - this is obviously a nonsense, but will get us some data in
    return fact_append_planner.create_local_dataframe_append_plan(
        read_version_target_model=read_version_target_model,
        write_version_target_model=write_version_target_model,
        source_metadata=source_metadata,
        column_mapping = column_mapping,
        read_version = read_version,
        write_version = write_version,
    )

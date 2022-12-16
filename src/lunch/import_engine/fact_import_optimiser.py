from pandas import DataFrame

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.fact_append_plan import FactAppendPlan
from src.lunch.import_engine.fact_append_planner import FactAppendPlanner
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.storage.fact_data_store import FactDataStore


class FactImportOptimiser(Conductor):
    def __init__(
        self,
        fact_append_planner: FactAppendPlanner,
        fact_data_store: FactDataStore,
        model_manager: ModelManager,
    ):
        self._fact_data_store = fact_data_store
        self._model_manager = model_manager
        self._fact_append_planner = fact_append_planner

    async def create_dataframe_append_plan(
        self,
        fact_name: str,
        data: DataFrame,
        read_version: Version,
        write_version: Version,
    ) -> FactAppendPlan:
        return await _create_dataframe_append_plan(
            fact_name=fact_name,
            data=data,
            read_version=read_version,
            write_version=write_version,
            fact_append_planner=self._fact_append_planner,
            model_manager=self._model_manager,
            fact_data_store=self._fact_data_store,
        )


async def _create_dataframe_append_plan(
    fact_name: str,
    data: DataFrame,
    read_version: Version,
    write_version: Version,
    fact_append_planner: FactAppendPlanner,
    model_manager: ModelManager,
    fact_data_store: FactDataStore,
) -> FactAppendPlan:
    # TODO - try to remove the FactDataStore - ideally, the FactDataStore will be able to handle
    #  generic instructions we give it

    # It is the model_manager's job to ensure it is handing out valid facts, so don't validate here
    read_fact = await model_manager.get_fact_by_name(
        name=fact_name, version=read_version, add_default_storage=True
    )
    write_fact = await model_manager.get_fact_by_name(
        name=fact_name, version=write_version, add_default_storage=True
    )

    # name vs. type/attributes?
    data_columns = {n: t for n, t in zip(data.columns, data.dtypes)}

    # TODO - Pass in rules about the capability of the engine - is the engine a grid, or local dask etc.
    #  This will influence the choices the optimiser makes
    # TODO - merge is hardcoded to the first column - this is obviously a nonsense, but will get us some data in
    return fact_append_planner.create_local_dataframe_append_plan(
        read_fact=read_fact,
        write_fact=write_fact,
        data_columns=data_columns,
        # a dict of functions that can be called to do the standard transformations
        # or to get e.g. filenames
        # The store is handing these out, so it had better understand them when they are
        # requested later
        read_fact_storage_instructions=fact_data_store.storage_instructions(
            read_version
        ),
        write_fact_storage_instructions=fact_data_store.storage_instructions(
            write_version
        ),
        read_filter={},
        merge_key=[0],
    )

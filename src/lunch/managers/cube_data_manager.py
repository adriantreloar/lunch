# TODO - I don't like these imports - it suggests that update_fact_from_dataframe is a method in the wrong place
#  Ideally a smidgeon will be left for typing, e.f. pd.DataFrame, dd.DataFrame etc.
import pandas as pd

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.import_engine.fact_import_optimiser import FactImportOptimiser
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.plans.plan import Plan
from src.lunch.storage.fact_data_store import FactDataStore


class CubeDataManager(Conductor):
    def __init__(
        self,
        model_manager: ModelManager,
        fact_data_store: FactDataStore,
        fact_import_optimiser: FactImportOptimiser,
        fact_import_enactor: FactImportEnactor,
    ):
        self._model_manager = model_manager

        self._fact_data_store = fact_data_store

        self._fact_import_optimiser = fact_import_optimiser
        self._fact_import_enactor = fact_import_enactor

    async def append_fact_from_dataframe(
        self,
        plan: Plan,
        source_data: pd.DataFrame,
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _append_fact_from_dataframe(
            plan=plan,
            source_data=source_data,
            read_version=read_version,
            write_version=write_version,
            fact_import_enactor=self._fact_import_enactor,
            fact_data_store=self._fact_data_store,
        )


async def _append_fact_from_dataframe(
    plan: Plan,
    source_data: pd.DataFrame,
    read_version: Version,
    write_version: Version,
    fact_import_enactor: FactImportEnactor,
    fact_data_store: FactDataStore,
) -> None:
    return await fact_import_enactor.enact_plan(
        plan, source_data, read_version, write_version, fact_data_store
    )

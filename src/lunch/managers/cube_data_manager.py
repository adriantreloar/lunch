# TODO - I don't like these imports - it suggests that update_fact_from_dataframe is a method in the wrong place
#  Ideally a smidgeon will be left for typing, e.f. pd.DataFrame, dd.DataFrame etc.
import pandas as pd

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.import_engine.fact_import_optimiser import FactImportOptimiser
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
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
        name: str,
        data: pd.DataFrame,
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _append_fact_from_dataframe(
            name=name,
            data=data,
            read_version=read_version,
            write_version=write_version,
            model_manager=self._model_manager,
            fact_import_optimiser=self._fact_import_optimiser,
            fact_import_enactor=self._fact_import_enactor,
            fact_data_store=self._fact_data_store,
        )


async def _append_fact_from_dataframe(
    name: str,
    data: pd.DataFrame,
    read_version: Version,
    write_version: Version,
    model_manager: ModelManager,
    fact_import_optimiser: FactImportOptimiser,
    fact_import_enactor: FactImportEnactor,
    fact_data_store: FactDataStore,
) -> None:
    # TODO - this is a sketched function
    #  everything is in the wrong place, but before we find a home for everything, we need to know what it is doing
    """
    Dilemma - for a small fact, we want to get the fact (or DF) out of cache and merge it
    However, even then we may wish to do this on a single remote core
    For a large fact we want to go whole hog


    Steps:

    Find out what the fact data at the write version thinks the columns are called
    Get the column ids at the read version and
        Turn each columnar attribute file into a pd. series

    Turn the serieses into a df?

    Merge in the supplied df

    Turn the final df series into column files

    """

    # TODO - either during plan creation, or here
    #  translate the read version to the version of the index of the reference data to the version fo the fact

    # Create import plan
    # We need to query storage (e.g. the indexes) for size hints, so that we can create a decent pland
    # TODO - at some point we may need statistics, to speed this sort of thing up
    import_plan = await fact_import_optimiser.create_dataframe_append_plan(
        fact_name=name,
        data=data,
        read_version=read_version,
        write_version=write_version,
    )

    # TODO - log import plan

    # Enact import plan
    return await fact_import_enactor.enact_plan(
        import_plan, data, read_version, write_version, fact_data_store
    )

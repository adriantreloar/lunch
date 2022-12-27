# TODO - I don't like these imports - it suggests that update_dimension_from_dataframe is a method in the wrong place
#  Ideally a smidgeon will be left for typing, e.f. pd.DataFrame, dd.DataFrame etc.
import pandas as pd

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.reference_data_store import ReferenceDataStore


class ReferenceDataManager(Conductor):
    def __init__(
        self,
        reference_data_store: ReferenceDataStore,
        dimension_data_store: DimensionDataStore,
        dimension_import_optimiser: DimensionImportOptimiser,
        dimension_import_enactor: DimensionImportEnactor,
    ):

        self._reference_data_store = reference_data_store
        self._dimension_data_store = dimension_data_store

        self._dimension_import_optimiser = dimension_import_optimiser
        self._dimension_import_enactor = dimension_import_enactor

    async def update_dimension_from_dataframe(
        self,
        name: str,
        data: pd.DataFrame,
        read_version: Version,
        write_version: Version,
    ) -> None:
        return await _update_dimension_from_dataframe(
            name=name,
            data=data,
            read_version=read_version,
            write_version=write_version,
            dimension_import_optimiser=self._dimension_import_optimiser,
            dimension_import_enactor=self._dimension_import_enactor,
            dimension_data_store=self._dimension_data_store,
        )


async def _update_dimension_from_dataframe(
    name: str,
    data: pd.DataFrame,
    read_version: Version,
    write_version: Version,
    dimension_import_optimiser: DimensionImportOptimiser,
    dimension_import_enactor: DimensionImportEnactor,
    dimension_data_store: DimensionDataStore,
) -> None:
    # TODO - this is a sketched function
    #  everything is in the wrong place, but before we find a home for everything, we need to know what it is doing
    """
    Dilemma - for a small dimension, we want to get the dimension (or DF) out of cache and merge it
    However, even then we may wish to do this on a single remote core
    For a large dimension we want to go whole hog


    Steps:

    Find out what the reference data at the write version thinks the columns are called
    Get the column ids at the read version and
        Turn each columnar attribute file into a pd. series

    Turn the serieses into a df?

    Merge in the supplied df

    Turn the final df series into attribute files

    """

    # TODO - either during plan creation, or here
    #  translate the read version to the version of the index of the reference data to the version fo the dimension

    # Create import plan
    # We need to query storage (e.g. the indexes) for size hints, so that we can create a decent pland
    # TODO - at some point we may need statistics, to speed this sort of thing up
    import_plan = await dimension_import_optimiser.create_dataframe_import_plan(
        dimension_name=name,
        data=data,
        read_version=read_version,
        write_version=write_version
    )

    # TODO - log import plan

    # Enact import plan
    return await dimension_import_enactor.enact_plan(
        import_plan=import_plan, data=data, read_version=read_version, write_version=write_version,
        dimension_data_store=dimension_data_store

    )

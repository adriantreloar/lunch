from lunch.base_classes.conductor import Conductor
from lunch.managers.model_manager import ModelManager
from lunch.storage.dimension_data_store import DimensionDataStore
from lunch.import_engine.dimension_import_planner import DimensionImportPlanner
from lunch.mvcc.version import Version
from pandas import DataFrame


class DimensionImportOptimiser(Conductor):

    def __init__(self,
                 dimension_import_planner: DimensionImportPlanner,
                 dimension_data_store: DimensionDataStore,
                 model_manager: ModelManager):
        self._dimension_data_store = dimension_data_store
        self._model_manager = model_manager
        self._dimension_import_planner = dimension_import_planner

    def create_dataframe_import_plan(self,
                                     dimension_name: str,
                                     data: DataFrame,
                                     read_version: Version,
                                     write_version: Version,
                                     ):
        return _create_dataframe_import_plan(
                                 dimension_name=dimension_name,
                                 data=data,
                                 read_version=read_version,
                                 write_version=write_version,
                                 dimension_import_planner=self._dimension_import_planner,
                                 model_manager=self._model_manager,
                                 dimension_data_store=self._dimension_data_store)

def _create_dataframe_import_plan(self,
                                 dimension_name: str,
                                 data: DataFrame,
                                 read_version: Version,
                                 write_version: Version,
                                 dimension_import_planner: DimensionImportPlanner,
                                 model_manager: ModelManager,
                                 dimension_data_store: DimensionDataStore):
    # TODO - try to remove the DimensionDataStore - ideally, the DimensionDataStore will be able to handle
    #  generic instructions we give it

    # TODO - The dimension_import_planner can keep throwing errors until it gets what it needs,
    #  or raise StopIteration once it has everything it needs

    # It is the model_manager's job to ensure it is handing out valid dimensions, so don't validate here
    read_dimension = model_manager.get_dimension(name=dimension_name, version=read_version, add_default_storage=True)
    write_dimension = model_manager.get_dimension(name=dimension_name, version=write_version, add_default_storage=True)

    # name vs. type/attributes?
    data_columns = data.columns

    # TODO - Pass in rules about the capability of the engine - is the engine a grid, or local dask etc.
    #  This will influence the choices the optimiser makes
    return dimension_import_planner.create_dataframe_import_plan(
                                read_dimension=read_dimension,
                                write_dimension=write_dimension,
                                data_columns=data_columns,
                                # a dict of functions that can be called to do the standard transformations
                                # or to get e.g. filenames
                                # The store is handing these out, so it had better understand them when they are
                                # requested later
                                dimension_storage_instructions=dimension_data_store.storage_instructions
                                )


from lunch.base_classes.conductor import Conductor
from lunch.managers.model_manager import ModelManager
from lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from lunch.import_engine.dimension_dataframe_merger import DimensionDataFrameMerger
from typing import Any

class ReferenceDataManager(Conductor):
    def __init__(
        self,
        model_manager: ModelManager,
        # TODO - these may want shuffling to other parts of the code
        #  e.g. a place specifically for operations on dimensions, and that would be imported here
        dimension_import_optimiser: DimensionImportOptimiser,
        dimension_dataframe_merger: DimensionDataFrameMerger,
    ):
        self._model_manager = model_manager
        self._dimension_import_optimiser = dimension_import_optimiser
        self._dimension_dataframe_merger = dimension_dataframe_merger

    async def enact_import_plan(self, import_plan: dict, import_sources: list[Any]):
        pass

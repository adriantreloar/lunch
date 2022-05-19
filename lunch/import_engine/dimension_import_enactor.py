from lunch.base_classes.conductor import Conductor

from lunch.import_engine.dimension_dataframe_merger import DimensionDataFrameMerger


class DimensionImportEnactor(Conductor):

    def __init__(
            self,
            dimension_dataframe_merger: DimensionDataFrameMerger
    ):
        self._dimension_dataframe_merger = dimension_dataframe_merger

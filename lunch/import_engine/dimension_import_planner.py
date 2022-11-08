from pandas import DataFrame

from lunch.base_classes.transformer import Transformer
from lunch.import_engine.dimension_import_plan import DimensionImportPlan


class DimensionImportPlanner(Transformer):
    def __init__(self):
        pass

    @staticmethod
    def create_dataframe_import_plan(
        read_dimension: dict,
        write_dimension: dict,
        read_dimension_storage_instructions: dict,
        write_dimension_storage_instructions: dict,
        data_columns: dict,  # name vs. type/attributes?
    ) -> DimensionImportPlan:
        """

        :param self:
        :param read_dimension:
        :param write_dimension:
        :param read_dimension_storage_instructions: From dimension storage.
            Allows the optimiser to know what instructions it has to work with
        :return:
        """

        return DimensionImportPlan(read_dimension=read_dimension,
                                    write_dimension=write_dimension,
                                    read_dimension_storage_instructions=read_dimension_storage_instructions,
                                    write_dimension_storage_instructions=write_dimension_storage_instructions,
                                    data_columns=data_columns)

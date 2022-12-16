from src.lunch.base_classes.transformer import Transformer
from src.lunch.plans.plan import Plan
from src.lunch.plans.basic_plan import BasicPlan


class DimensionImportPlanner(Transformer):
    def __init__(self):
        pass

    @staticmethod
    def create_local_dataframe_import_plan(
        read_dimension: dict,
        write_dimension: dict,
        read_dimension_storage_instructions: dict,
        write_dimension_storage_instructions: dict,
        data_columns: dict,  # name vs. type/attributes?
        read_filter: dict,
        merge_key: list,
    ) -> Plan:
        """

        :param self:
        :param read_dimension:
        :param write_dimension:
        :param read_dimension_storage_instructions: From dimension storage.
            Allows the optimiser to know what instructions it has to work with
        :return:
        """

        return BasicPlan(
            name="_import_locally_from_dataframe",
            inputs={"read_dimension": read_dimension,
                    "write_dimension": write_dimension,
                    "read_filter":read_filter,
                    "merge_key":merge_key,
                    "read_dimension_storage_instructions": read_dimension_storage_instructions,
                    "write_dimension_storage_instructions": write_dimension_storage_instructions,
                    "data_columns": data_columns
                    },
            outputs={}
        )

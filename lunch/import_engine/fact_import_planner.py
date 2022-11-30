from pandas import DataFrame

from lunch.base_classes.transformer import Transformer
from lunch.import_engine.fact_import_plan import FactImportPlan


class FactImportPlanner(Transformer):
    def __init__(self):
        pass

    @staticmethod
    def create_dataframe_import_plan(
        read_fact: dict,
        write_fact: dict,
        read_fact_storage_instructions: dict,
        write_fact_storage_instructions: dict,
        data_columns: dict,  # name vs. type/attributes?
        read_filter: dict,
        merge_key: list,
    ) -> FactImportPlan:
        """

        :param self:
        :param read_fact:
        :param write_fact:
        :param read_fact_storage_instructions: From fact storage.
            Allows the optimiser to know what instructions it has to work with
        :return:
        """

        return FactImportPlan(
            read_fact=read_fact,
            write_fact=write_fact,
            read_filter=read_filter,
            merge_key=merge_key,
            read_fact_storage_instructions=read_fact_storage_instructions,
            write_fact_storage_instructions=write_fact_storage_instructions,
            data_columns=data_columns,
        )

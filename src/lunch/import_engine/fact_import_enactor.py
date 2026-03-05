from typing import Any

from numpy import dtype

from src.lunch.base_classes.conductor import Conductor
from src.lunch.plans.plan import Plan
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.mvcc.version import Version
from src.lunch.import_engine.transformers.fact_dataframe_transformer import (
    FactDataFrameTransformer,
)
from src.lunch.storage.fact_data_store import FactDataStore


class FactImportEnactor(Conductor):
    def __init__(self):
        pass

    async def enact_plan(
        self,
        append_plan: Plan,
        data: Any,
        read_version: Version,
        write_version: Version,
        fact_data_store: FactDataStore,
    ):
        return await _enact_plan(
            append_plan=append_plan,
            data=data,
            read_version=read_version,
            write_version=write_version,
            fact_data_store=fact_data_store,
        )


async def _enact_plan(
    append_plan: Plan,
    data: Any,
    read_version: Version,
    write_version: Version,
    fact_data_store: FactDataStore,
):
    if isinstance(append_plan, BasicPlan) and append_plan.name == "_import_fact_append_locally_from_dataframe":
        # TODO: if import_plan.inputs["read_filter"] is a guid, lookup the output form a previous step
        # TODO: if import_plan.inputs["merge_key"] is a guid, lookup the output form a previous step

        await _import_fact_append_locally_from_dataframe(data=data,
                                             read_version=read_version,
                                             write_version=write_version,
                                             append_plan= append_plan,
                                             fact_data_store=fact_data_store)
    else:
        raise ValueError(append_plan)

async def _import_fact_append_locally_from_dataframe(data: Any,
                                             read_version: Version,
                                             write_version: Version,
                                             append_plan: Plan,
                                             fact_data_store: FactDataStore) -> None:
    read_fact = append_plan.inputs["read_fact"]
    column_id_mapping = append_plan.inputs["column_id_mapping"]
    merge_key = append_plan.inputs["merge_key"]
    read_filter = append_plan.inputs["read_filter"]
    write_fact = append_plan.outputs["write_fact"]

    renamed_df = data.rename(columns=column_id_mapping)

    column_types = {col_id: dtype(str) for col_id in column_id_mapping.values()}

    try:
        read_columns = await fact_data_store.get_columns(
            read_version=read_version,
            fact_id=read_fact.fact_id,
            filter=read_filter,
            column_types=column_types,
        )
    except KeyError:
        # First import — no existing data for this fact
        merged_df = renamed_df
    else:
        compare_df = FactDataFrameTransformer.make_dataframe(
            columns=read_columns, dtypes=column_types
        )
        merged_df = FactDataFrameTransformer.merge(
            source_df=renamed_df, compare_df=compare_df, key=merge_key
        )

    columnar_data = FactDataFrameTransformer.columnize(data=merged_df)

    await fact_data_store.put(
        fact_id=write_fact.fact_id,
        columnar_data=columnar_data,
        write_version=write_version,
        read_version=read_version,
    )

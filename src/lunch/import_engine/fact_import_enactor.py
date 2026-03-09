from typing import Any
from uuid import UUID

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.transformers.fact_dataframe_transformer import (
    FactDataFrameTransformer,
)
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.plan import Plan
from src.lunch.plans.serial_plan import SerialPlan
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


def _resolve_inputs(inputs: dict, output_store: dict) -> dict:
    return {k: output_store[v] if isinstance(v, UUID) else v for k, v in inputs.items()}


def _collect_outputs(outputs: dict, result: dict, output_store: dict) -> None:
    for key, handle in outputs.items():
        if isinstance(handle, UUID) and key in result:
            output_store[handle] = result[key]


async def _enact_plan(
    append_plan: Plan,
    data: Any,
    read_version: Version,
    write_version: Version,
    fact_data_store: FactDataStore,
) -> dict:
    if isinstance(append_plan, SerialPlan):
        output_store: dict = {}
        for step in append_plan.steps:
            if isinstance(step, BasicPlan):
                resolved = BasicPlan(
                    name=step.name,
                    inputs=_resolve_inputs(step.inputs, output_store),
                    outputs=step.outputs,
                )
            else:
                resolved = step
            result = await _enact_plan(resolved, data, read_version, write_version, fact_data_store)
            _collect_outputs(step.outputs, result, output_store)
        return {}
    elif isinstance(append_plan, BasicPlan) and append_plan.name == "_import_fact_append_locally_from_dataframe":
        return await _import_fact_append_locally_from_dataframe(
            data=data,
            read_version=read_version,
            write_version=write_version,
            append_plan=append_plan,
            fact_data_store=fact_data_store,
        )
    else:
        raise ValueError(append_plan)


async def _import_fact_append_locally_from_dataframe(
    data: Any, read_version: Version, write_version: Version, append_plan: Plan, fact_data_store: FactDataStore
) -> dict:
    read_fact = append_plan.inputs["read_fact"]
    column_id_mapping = append_plan.inputs["column_id_mapping"]
    merge_key = append_plan.inputs["merge_key"]
    read_filter = append_plan.inputs["read_filter"]
    write_fact = append_plan.outputs["write_fact"]

    renamed_df = FactDataFrameTransformer.rename(data, column_id_mapping)
    column_types = FactDataFrameTransformer.column_types_from_mapping(column_id_mapping)

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
        compare_df = FactDataFrameTransformer.make_dataframe(columns=read_columns, dtypes=column_types)
        merged_df = FactDataFrameTransformer.merge(source_df=renamed_df, compare_df=compare_df, key=merge_key)

    columnar_data = FactDataFrameTransformer.columnize(data=merged_df)

    await fact_data_store.put(
        fact_id=write_fact.fact_id,
        columnar_data=columnar_data,
        write_version=write_version,
        read_version=read_version,
    )
    return {"write_fact": write_fact}

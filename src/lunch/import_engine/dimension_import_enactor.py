import asyncio
from typing import Any
from uuid import UUID

from src.lunch.base_classes.conductor import Conductor
from src.lunch.import_engine.transformers.dimension_dataframe_transformer import (
    DimensionDataFrameTransformer,
)
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.plans.plan import Plan
from src.lunch.storage.dimension_data_store import DimensionDataStore


class DimensionImportEnactor(Conductor):
    def __init__(self):
        pass

    async def enact_plan(
        self,
        import_plan: Plan,
        data: Any,
        read_version: Version,
        write_version: Version,
        dimension_data_store: DimensionDataStore,
    ):
        return await _enact_plan(
            import_plan=import_plan,
            data=data,
            read_version=read_version,
            write_version=write_version,
            dimension_data_store=dimension_data_store,
        )


def _resolve_inputs(inputs: dict, output_store: dict) -> dict:
    return {k: output_store[v] if isinstance(v, UUID) else v for k, v in inputs.items()}


def _collect_outputs(outputs: dict, result: dict, output_store: dict) -> None:
    for key, handle in outputs.items():
        if isinstance(handle, UUID) and key in result:
            output_store[handle] = result[key]


async def _enact_plan(
    import_plan: Plan,
    data: Any,
    read_version: Version,
    write_version: Version,
    dimension_data_store: DimensionDataStore,
) -> dict:
    if isinstance(import_plan, DagPlan):
        result_registry: dict = {}
        executed: set = set()
        while len(executed) < len(import_plan.nodes):
            ready = [
                node_id
                for node_id, node in import_plan.nodes.items()
                if node_id not in executed
                and all(v in result_registry for v in node.inputs.values() if isinstance(v, UUID))
            ]
            if not ready:
                break
            results = await asyncio.gather(
                *[
                    _enact_plan(
                        BasicPlan(
                            name=import_plan.nodes[node_id].name,
                            inputs=_resolve_inputs(import_plan.nodes[node_id].inputs, result_registry),
                            outputs=import_plan.nodes[node_id].outputs,
                        ),
                        data,
                        read_version,
                        write_version,
                        dimension_data_store,
                    )
                    for node_id in ready
                ]
            )
            for node_id, result in zip(ready, results):
                _collect_outputs(import_plan.nodes[node_id].outputs, result, result_registry)
                executed.add(node_id)
        return {}
    elif isinstance(import_plan, BasicPlan) and import_plan.name == "_import_locally_from_dataframe":
        return await _import_locally_from_dataframe(
            data=data,
            read_version=read_version,
            write_version=write_version,
            read_dimension=import_plan.inputs["read_dimension"],
            write_dimension=import_plan.inputs["write_dimension"],
            dimension_data_store=dimension_data_store,
            read_filter=import_plan.inputs["read_filter"],
            merge_key=import_plan.inputs["merge_key"],
        )
    else:
        raise ValueError(import_plan)


async def _import_locally_from_dataframe(
    data: Any,
    read_version: Version,
    write_version: Version,
    read_dimension: dict,
    write_dimension: dict,
    read_filter: dict,
    merge_key: dict,
    dimension_data_store: DimensionDataStore,
) -> dict:

    # TODO assert data is of the type specified in the plan
    #  could be a dataframe, dask dataframe, list of open files, list of file names, file mask, and so on

    # TODO we have started with bare instructions, turn into orchestrated thing later

    # TODO NEXT - check this is working - if so, see what isn't running in the larger script

    # TODO - we have bare transformations here
    #  we need an import_plan transformer class
    #  which means we really need an ImportPlan named-dict

    if read_dimension is None:
        # The dimension  only exists for the write version,
        # it must have been created as part of the write transaction
        merged_df = data
    else:
        column_types = DimensionDataFrameTransformer.column_types_from_dimension(read_dimension)
        # Could throw a key error so keep this lookup outside the next try block
        dimension_id = read_dimension["id_"]

        try:
            read_columns = await dimension_data_store.get_columns(
                read_version=read_version,
                dimension_id=dimension_id,
                filter=read_filter,
                column_types=column_types,
            )
        except KeyError:
            # The first time we have had data for this dimension
            merged_df = data
        else:
            # We are making the
            compare_df = DimensionDataFrameTransformer.make_dataframe(columns=read_columns, dtypes=column_types)

            merged_df = DimensionDataFrameTransformer.merge(source_df=data, compare_df=compare_df, key=merge_key)

    # dictionary of columns? attribute_id : column/iterator
    # how to represent index?
    columnar_data = DimensionDataFrameTransformer.columnize(data=merged_df)

    # put will have to handle its indexes too
    await dimension_data_store.put(
        dimension_id=write_dimension["id_"],
        columnar_data=columnar_data,
        write_version=write_version,
        read_version=read_version,
    )
    return {"write_dimension": write_dimension}

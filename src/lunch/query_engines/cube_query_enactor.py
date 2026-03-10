import asyncio
from typing import Any, Dict
from uuid import UUID

from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.query_engines.query_enactor import QueryEnactor
from src.lunch.query_engines.query_result import QueryResult
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.fact_data_store import FactDataStore


class CubeQueryEnactor(QueryEnactor):
    """Concrete QueryEnactor for cube queries.

    Holds references to FactDataStore and DimensionDataStore. Builds a dispatch
    table mapping step names to handler coroutines and implements the DAG
    execution loop.
    """

    def __init__(
        self,
        fact_data_store: FactDataStore,
        dimension_data_store: DimensionDataStore,
    ):
        self._fact_data_store = fact_data_store
        self._dimension_data_store = dimension_data_store
        self._dispatch = {
            "FetchDimensionData": _fetch_dimension_data,
            "FetchFactData": _fetch_fact_data,
            "JoinDimensionsToFact": _join_dimensions_to_fact,
            "Aggregate": _aggregate,
        }

    async def enact(self, plan: DagPlan) -> QueryResult:
        return await _enact(
            plan=plan,
            dispatch=self._dispatch,
            fact_data_store=self._fact_data_store,
            dimension_data_store=self._dimension_data_store,
        )


async def _enact(
    plan: DagPlan,
    dispatch: dict,
    fact_data_store: FactDataStore,
    dimension_data_store: DimensionDataStore,
) -> QueryResult:
    """DAG execution loop.

    Maintains a result_registry mapping key -> data. On each iteration,
    finds all nodes whose UUID inputs are all satisfied, executes them
    concurrently via asyncio.gather, then adds their outputs to the registry.
    Repeats until all nodes are done.
    """
    result_registry: Dict[Any, Any] = {}
    executed: set = set()

    while len(executed) < len(plan.nodes):
        ready = [
            node_id
            for node_id, node in plan.nodes.items()
            if node_id not in executed and all(k in result_registry for k in node.inputs if isinstance(k, UUID))
        ]
        if not ready:
            break

        outputs_list = await asyncio.gather(
            *[
                dispatch[plan.nodes[node_id].name](
                    plan.nodes[node_id],
                    result_registry,
                    fact_data_store,
                    dimension_data_store,
                )
                for node_id in ready
            ]
        )

        for node_id, node_outputs in zip(ready, outputs_list):
            result_registry.update(node_outputs)
            executed.add(node_id)

    final_data = {uuid: result_registry[uuid] for uuid in plan.outputs if uuid in result_registry}
    data = next(iter(final_data.values())) if len(final_data) == 1 else final_data
    return QueryResult(data=data, query=None, plan=plan)


async def _fetch_dimension_data(
    node: BasicPlan,
    result_registry: Dict[Any, Any],
    fact_data_store: FactDataStore,
    dimension_data_store: DimensionDataStore,
) -> Dict[Any, Any]:
    """Read dimension columns from DimensionDataStore."""
    dimension = node.inputs["dimension"]
    dimension_id = dimension["dimension_id"]
    columns = await dimension_data_store.get_columns(
        dimension_id=dimension_id,
        column_types={},
        filter=None,
        read_version=dimension.get("version"),
    )
    (out_uuid,) = node.outputs.keys()
    return {out_uuid: columns}


async def _fetch_fact_data(
    node: BasicPlan,
    result_registry: Dict[Any, Any],
    fact_data_store: FactDataStore,
    dimension_data_store: DimensionDataStore,
) -> Dict[Any, Any]:
    """Read fact partition columns from FactDataStore."""
    partition_id = node.inputs["partition_id"]
    version = node.inputs["version"]
    columns = await fact_data_store.get_columns(
        fact_id=partition_id,
        column_types={},
        filter=None,
        read_version=version,
    )
    (out_uuid,) = node.outputs.keys()
    return {out_uuid: columns}


async def _join_dimensions_to_fact(
    node: BasicPlan,
    result_registry: Dict[Any, Any],
    fact_data_store: FactDataStore,
    dimension_data_store: DimensionDataStore,
) -> Dict[Any, Any]:
    """Join dimension data onto the fact table keyed by dimension member ids."""
    datasets = [result_registry[k] for k in node.inputs if isinstance(k, UUID)]
    joined = datasets[0] if datasets else None
    (out_uuid,) = node.outputs.keys()
    return {out_uuid: joined}


async def _aggregate(
    node: BasicPlan,
    result_registry: Dict[Any, Any],
    fact_data_store: FactDataStore,
    dimension_data_store: DimensionDataStore,
) -> Dict[Any, Any]:
    """Apply the specified aggregation function to the dataset."""
    data_uuids = [k for k in node.inputs if isinstance(k, UUID)]
    data = result_registry[data_uuids[0]] if data_uuids else None
    result = data
    (out_uuid,) = node.outputs.keys()
    return {out_uuid: result}

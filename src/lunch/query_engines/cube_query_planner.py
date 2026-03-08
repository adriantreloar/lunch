from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_dag_builder import CubeQueryDagBuilder
from src.lunch.query_engines.dag_plan import DagPlan
from src.lunch.query_engines.planner import Planner
from src.lunch.storage.fact_data_store import FactDataStore


class CubeQueryPlanner(Planner):
    """Concrete Planner for cube queries.

    Fetches the partition manifest from FactDataStore, then delegates
    DAG construction (pure, no I/O) to CubeQueryDagBuilder.
    """

    def __init__(
        self,
        fact_data_store: FactDataStore,
        cube_query_dag_builder: CubeQueryDagBuilder,
    ):
        self._fact_data_store = fact_data_store
        self._cube_query_dag_builder = cube_query_dag_builder

    async def plan(self, query: FullySpecifiedFactQuery) -> DagPlan:
        return await _plan(
            query=query,
            fact_data_store=self._fact_data_store,
            cube_query_dag_builder=self._cube_query_dag_builder,
        )


async def _plan(
    query: FullySpecifiedFactQuery,
    fact_data_store: FactDataStore,
    cube_query_dag_builder: CubeQueryDagBuilder,
) -> DagPlan:
    partitions = await fact_data_store.get_partition_manifest(version=query.version)
    return CubeQueryDagBuilder.build(query=query, partitions=partitions)

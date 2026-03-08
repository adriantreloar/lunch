from src.lunch.queries.cube_query import CubeQuery
from src.lunch.query_engines.cube_query_enactor import CubeQueryEnactor
from src.lunch.query_engines.cube_query_planner import CubeQueryPlanner
from src.lunch.query_engines.cube_query_specifier import CubeQuerySpecifier
from src.lunch.query_engines.query_engine import QueryEngine
from src.lunch.query_engines.query_result import QueryResult


class CubeQueryEngine(QueryEngine):
    """Concrete QueryEngine for cube queries.

    Wires together a CubeQuerySpecifier, a CubeQueryPlanner, and a
    CubeQueryEnactor into the full pipeline:

        CubeQuery -> specifier -> FullySpecifiedFactQuery
                  -> planner  -> DagPlan
                  -> enactor  -> QueryResult
    """

    def __init__(
        self,
        specifier: CubeQuerySpecifier,
        planner: CubeQueryPlanner,
        enactor: CubeQueryEnactor,
    ):
        self._specifier = specifier
        self._planner = planner
        self._enactor = enactor

    async def query(self, query: CubeQuery) -> QueryResult:
        return await _query(
            query=query,
            specifier=self._specifier,
            planner=self._planner,
            enactor=self._enactor,
        )


async def _query(
    query: CubeQuery,
    specifier: CubeQuerySpecifier,
    planner: CubeQueryPlanner,
    enactor: CubeQueryEnactor,
) -> QueryResult:
    fully_specified = await specifier.specify(query)
    plan = await planner.plan(fully_specified)
    return await enactor.enact(plan)

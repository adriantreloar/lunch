from src.lunch.managers.model_manager import ModelManager
from src.lunch.managers.version_manager import VersionManager
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_resolver import CubeQueryResolver
from src.lunch.query_engines.query_specifier import QuerySpecifier


class CubeQuerySpecifier(QuerySpecifier):
    """Conductor that specifies a CubeQuery into a FullySpecifiedFactQuery.

    Steps:
    1. Resolve the version (calls VersionManager if query.version is 'latest').
    2. Fetch the StarSchema from ModelManager.
    3. Delegate resolution to CubeQueryResolver (pure, no I/O).
    """

    def __init__(
        self,
        version_manager: VersionManager,
        model_manager: ModelManager,
        cube_query_resolver: CubeQueryResolver,
    ):
        self._version_manager = version_manager
        self._model_manager = model_manager
        self._cube_query_resolver = cube_query_resolver

    async def specify(self, query: CubeQuery) -> FullySpecifiedFactQuery:
        return await _specify(
            query=query,
            version_manager=self._version_manager,
            model_manager=self._model_manager,
        )


async def _specify(
    query: CubeQuery,
    version_manager: VersionManager,
    model_manager: ModelManager,
) -> FullySpecifiedFactQuery:
    if query.version == "latest":
        version = await version_manager.read_version()
    else:
        version = query.version

    star_schema = await model_manager.get_star_schema_model_by_fact_name(
        name=query.star_schema_name,
        version=version,
    )

    return CubeQueryResolver.resolve(
        query=query,
        version=version,
        star_schema=star_schema,
    )

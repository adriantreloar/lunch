from typing import Any, List

from src.lunch.base_classes.transformer import Transformer
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.resolved_cube_query import ResolvedCubeQuery


class CubeQueryContextResolver(Transformer):
    """Resolves vague context fields in a CubeQuery, returning a ResolvedCubeQuery.

    Shorthand mappings:
    - ``projection='default'`` → ``{"dimensions": [...], "measures": [...]}``
      where dimensions and measures are derived from the StarSchema.
      Any other value is passed through unchanged.
    - ``aggregation='default'`` → ``["sum"]``
      Any other value is passed through as a list.

    The ``version`` parameter is the already-resolved ``Version`` object (the
    caller is responsible for resolving ``"latest"`` before calling this method).
    The ``filter`` field of the ``CubeQuery`` is not carried into
    ``ResolvedCubeQuery`` — filter resolution is handled downstream.
    """

    @staticmethod
    def resolve(
        query: CubeQuery,
        version: Version,
        star_schema: StarSchema,
    ) -> ResolvedCubeQuery:
        projection = _resolve_projection(query.projection, star_schema)
        aggregation = _resolve_aggregation(query.aggregation)
        return ResolvedCubeQuery(
            star_schema=star_schema,
            version=version,
            projection=projection,
            aggregation=aggregation,
        )


def _resolve_projection(projection: Any, star_schema: StarSchema) -> dict:
    if projection == "default":
        dimensions = [{"dimension_id": dim_id, **dim} for dim_id, dim in star_schema.dimensions.items()]
        measures: List[int] = [m.measure_id for m in star_schema.fact.measures]
        return {"dimensions": dimensions, "measures": measures}
    return projection


def _resolve_aggregation(aggregation: Any) -> list:
    if aggregation == "default":
        return ["sum"]
    return list(aggregation)

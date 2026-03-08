from typing import List

from src.lunch.base_classes.transformer import Transformer
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery


class CubeQueryResolver(Transformer):
    """Resolves a CubeQuery into a FullySpecifiedFactQuery using a Version and StarSchema.

    Maps shorthand values:
    - ``projection='default'``  -> all dimensions and measures from the star_schema
    - ``aggregation='default'`` -> ``['sum']``
    - ``filter=None``           -> ``[]``

    For projection, 'default' means both dimensions and measures are derived from the
    StarSchema.  Any other value is treated as an explicit dict with keys 'dimensions'
    (List[dict]) and 'measures' (List[int]) which are passed through unchanged.

    Explicit aggregation and filter values are passed through as lists.
    """

    @staticmethod
    def resolve(
        query: CubeQuery,
        version: Version,
        star_schema: StarSchema,
    ) -> FullySpecifiedFactQuery:
        dimensions = _resolve_dimensions(query.projection, star_schema)
        measures = _resolve_measures(query.projection, star_schema)
        filters = _resolve_filters(query.filter)
        aggregations = _resolve_aggregations(query.aggregation)

        return FullySpecifiedFactQuery(
            star_schema=star_schema,
            version=version,
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            aggregations=aggregations,
        )


def _resolve_dimensions(projection, star_schema: StarSchema) -> List[dict]:
    if projection == "default":
        return [{"dimension_id": dim_id, **dim} for dim_id, dim in star_schema.dimensions.items()]
    return list(projection["dimensions"])


def _resolve_measures(projection, star_schema: StarSchema) -> List[int]:
    if projection == "default":
        return [m.measure_id for m in star_schema.fact.measures]
    return list(projection["measures"])


def _resolve_filters(filter_value) -> list:
    if filter_value is None:
        return []
    return list(filter_value)


def _resolve_aggregations(aggregation) -> list:
    if aggregation == "default":
        return ["sum"]
    return list(aggregation)

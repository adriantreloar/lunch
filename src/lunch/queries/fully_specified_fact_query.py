from typing import Any, List

from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.query import Query


class FullySpecifiedFactQuery(Query):
    """Resolved query with all fields concrete and unambiguous.

    Output of QuerySpecifier when given a CubeQuery. This is the input
    to the QueryPlanner.
    """

    def __init__(
        self,
        star_schema: StarSchema,
        version: Version,
        dimensions: List[dict],
        measures: List[int],
        filters: List[Any],
        aggregations: List[Any],
    ):
        self.star_schema: StarSchema = star_schema
        self.version: Version = version
        self.dimensions: List[dict] = dimensions
        self.measures: List[int] = measures
        self.filters: List[Any] = filters
        self.aggregations: List[Any] = aggregations

from typing import Any, Optional

from src.lunch.queries.query import Query


class CubeQuery(Query):
    """Vague caller request for cube data.

    Fields such as version and projection may use shorthand values like
    'latest' or 'default'.
    """

    def __init__(
        self,
        star_schema_name: str,
        version: Any,
        projection: Any,
        filter: Optional[Any],
        aggregation: Any,
    ):
        self.star_schema_name: str = star_schema_name
        self.version: Any = version
        self.projection: Any = projection
        self.filter: Optional[Any] = filter
        self.aggregation: Any = aggregation

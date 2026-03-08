from typing import Any

from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.query import Query


class ResolvedCubeQuery(Query):
    """Enriched cube query produced by CubeQueryContextResolver.

    All vague fields (e.g. 'latest', 'default') have been replaced with
    explicit concrete values.  Carries an explicit Version and StarSchema.
    """

    def __init__(
        self,
        star_schema: StarSchema,
        version: Version,
        projection: Any,
        aggregation: Any,
    ):
        self.star_schema: StarSchema = star_schema
        self.version: Version = version
        self.projection: Any = projection
        self.aggregation: Any = aggregation

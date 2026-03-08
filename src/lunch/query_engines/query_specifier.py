from abc import abstractmethod

from src.lunch.base_classes.conductor import Conductor
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.queries.query import Query


class QuerySpecifier(Conductor):
    """Abstract Conductor that converts a vague Query into a FullySpecifiedFactQuery."""

    @abstractmethod
    async def specify(self, query: Query) -> FullySpecifiedFactQuery:
        ...

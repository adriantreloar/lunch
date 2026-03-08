from abc import abstractmethod

from src.lunch.base_classes.conductor import Conductor
from src.lunch.queries.query import Query
from src.lunch.query_engines.query_result import QueryResult


class QueryEngine(Conductor):
    """Abstract Conductor that orchestrates the full query pipeline.

    Holds references to a QuerySpecifier, a Planner, and a QueryEnactor.
    Defines: async def query(self, query: Query) -> QueryResult.

    Subclasses inject the appropriate typed collaborators via their constructors.
    """

    @abstractmethod
    async def query(self, query: Query) -> QueryResult: ...

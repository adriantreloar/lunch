from abc import abstractmethod

from src.lunch.base_classes.conductor import Conductor
from src.lunch.query_engines.dag_plan import DagPlan
from src.lunch.query_engines.query_result import QueryResult


class QueryEnactor(Conductor):
    """Abstract Conductor base for all query enactors.

    Defines the enact interface. Subclasses inject the appropriate data stores
    and Transformer helpers via their constructors.
    """

    @abstractmethod
    async def enact(self, plan: DagPlan) -> QueryResult: ...

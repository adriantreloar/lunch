from abc import abstractmethod

from src.lunch.base_classes.conductor import Conductor
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.plans.dag_plan import DagPlan


class Planner(Conductor):
    """Abstract Conductor that converts a FullySpecifiedFactQuery into a DagPlan."""

    @abstractmethod
    async def plan(self, query: FullySpecifiedFactQuery) -> DagPlan: ...

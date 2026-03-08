from typing import Any, Optional

from src.lunch.base_classes.data import Data
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.dag_plan import DagPlan


class QueryResult(Data):
    """Pure data container returned by the enactor.

    data: the result dataset (e.g. a pandas DataFrame or Arrow Table)
    query: the FullySpecifiedFactQuery that produced this result (None when the
        enactor was not given the original query, e.g. enact(plan) only)
    plan: the DagPlan that was enacted, for debugging and profiling
    """

    def __init__(
        self,
        data: Any,
        query: Optional[FullySpecifiedFactQuery],
        plan: DagPlan,
    ):
        self.data: Any = data
        self.query: Optional[FullySpecifiedFactQuery] = query
        self.plan: DagPlan = plan

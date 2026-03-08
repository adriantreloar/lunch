from typing import Set
from uuid import UUID

from src.lunch.queries.basic_query import BasicQuery
from src.lunch.queries.query import Query


class SerialQuery(Query):
    """An ordered sequence of BasicQuery steps with UUID output references.

    Mirrors SerialPlan in structure.  Steps are executed in order; later steps
    may reference outputs of earlier steps by UUID.  A pure data container —
    carries no behaviour.

    steps: ordered list of BasicQuery steps to execute
    outputs: set of UUIDs identifying the final result datasets
    """

    def __init__(
        self,
        steps: list[BasicQuery],
        outputs: Set[UUID],
    ):
        self.steps: list[BasicQuery] = steps
        self.outputs: Set[UUID] = outputs

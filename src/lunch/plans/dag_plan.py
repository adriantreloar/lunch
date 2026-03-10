from typing import Set, Tuple
from uuid import UUID

from src.lunch.base_classes.data import Data
from src.lunch.plans.basic_plan import BasicPlan


class DagPlan(Data):
    """Pure data container representing a directed acyclic graph plan.

    nodes: mapping from node UUID to BasicPlan
    edges: set of (from_node_uuid, to_node_uuid) pairs expressing dependencies
    inputs: set of UUIDs of externally-supplied datasets (typically empty for query plans)
    outputs: set of UUIDs of the datasets that constitute the final result
    """

    def __init__(
        self,
        nodes: dict[UUID, BasicPlan],
        edges: Set[Tuple[UUID, UUID]],
        inputs: Set[UUID],
        outputs: Set[UUID],
    ):
        self.nodes: dict[UUID, BasicPlan] = nodes
        self.edges: Set[Tuple[UUID, UUID]] = edges
        self.inputs: Set[UUID] = inputs
        self.outputs: Set[UUID] = outputs

    def __repr__(self) -> str:
        return f"DagPlan(nodes={self.nodes!r}, edges={self.edges!r})"

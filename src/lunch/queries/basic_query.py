from typing import Any

from src.lunch.queries.query import Query


class BasicQuery(Query):
    """A single named query step with typed inputs and outputs.

    Mirrors BasicPlan in structure.  A pure data container — carries no
    behaviour.

    name: the step name (e.g. 'FetchDimensionData')
    inputs: mapping of parameter name / UUID to value or data reference
    outputs: mapping of output name / UUID to value or data reference
    """

    def __init__(
        self,
        name: str,
        inputs: dict[str, Any],
        outputs: dict[str, Any],
    ):
        self.name: str = name
        self.inputs: dict[str, Any] = inputs
        self.outputs: dict[str, Any] = outputs

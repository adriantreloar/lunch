from typing import Any

from src.lunch.plans.plan import Plan


class BasicPlan(Plan):
    """Query plan which is simply an instruction to call a local function
    Name, name of function
    Outputs, name of output to value. The value may be a uuid handle (future) or an actual object (current usage).
    Inputs, name of function parameter, with a value, or a uuid, where the value should be supplied by a previous output
    """

    def __init__(self, name: str, inputs: dict[str, Any], outputs: dict[str, Any]):
        self.name: str = name
        self.inputs: dict[str, Any] = inputs
        self.outputs: dict[str, Any] = outputs

    def __repr__(self) -> str:
        inputs = list(self.inputs.keys())
        outputs = list(self.outputs.keys())
        return f"BasicPlan({self.name!r}, inputs={inputs}, outputs={outputs})"

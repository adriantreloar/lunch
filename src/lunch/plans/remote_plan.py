from typing import Any

from src.lunch.plans.plan import Plan


class RemotePlan(Plan):
    """Query plan which is an instruction to call a remote RPC service
    Location, address of the service
    Name, name of remote procedure
    Outputs, name of output to value. The value may be a uuid handle (future) or an actual object (current usage).
    Inputs, name of function parameter, with a value, or a uuid, where the value should be supplied by a previous output
    """

    def __init__(self, location: str, name: str, inputs: dict[str, Any], outputs: dict[str, Any]):
        self.location: str = location
        self.name: str = name
        self.inputs: dict[str, Any] = inputs
        self.outputs: dict[str, Any] = outputs

    def __repr__(self) -> str:
        inputs = list(self.inputs.keys())
        outputs = list(self.outputs.keys())
        return f"RemotePlan({self.location!r}, {self.name!r}, inputs={inputs}, outputs={outputs})"

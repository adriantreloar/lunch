from typing import Any
from src.lunch.plans.plan import Plan
from uuid import uuid1

class RemotePlan(Plan):
    """Query plan which is an instruction to call a remote RPC service
    Location, address of the service
    Name, name of remote procedure
    Outputs, name of output to uuid. The uuid can be used as a reference to push the output into an input elsewhere
    in a complex plan.
    Inputs, name of function parameter, with a value, or a uuid, where the value should be supplied by a previous output
    """

    def __init__(self, location: str, name: str, inputs: dict[str, Any], outputs: dict[str, uuid1]):
        self.location: str = location
        self.name: str = name
        self.inputs: dict[str, Any] = inputs
        self.outputs: dict[str, uuid1] = outputs

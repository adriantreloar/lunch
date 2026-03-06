from src.lunch.plans.plan import Plan

class SerialPlan(Plan):
    """A list of query plan items that must be executed sequentially
    """

    def __init__(self, steps: list[Plan]):
        self.steps = steps

    def __repr__(self) -> str:
        return f"SerialPlan({self.steps!r})"
from src.lunch.plans.plan import Plan

class ParallelPlan(Plan):
    """A list of query plan items that may be executed in parallel
    """

    def __init__(self, steps: list[Plan]):
        self.steps = steps

    def __repr__(self) -> str:
        return f"ParallelPlan({self.steps!r})"

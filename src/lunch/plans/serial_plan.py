from src.lunch.plans.plan import Plan

class SerialPlan(Plan):
    """A list of query plan items that must be executed sequentially
    """

    def __init__(self, steps: list[Plan]):
        self.steps = steps
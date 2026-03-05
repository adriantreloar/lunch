from src.lunch.base_classes.transformer import Transformer
from src.lunch.model.fact import Fact


class FactComparer(Transformer):
    """Static methods to compare two Fact objects"""

    @staticmethod
    def compare(previous_fact: Fact | None, new_fact: Fact) -> dict:
        """
        Create a report detailing the differences between two facts.
        If previous_fact is None (first write), no deletions are possible.

        Returns a dict with:
          - removed_dimension_ids: dimension_ids present in previous_fact but not new_fact
          - added_dimension_ids:   dimension_ids present in new_fact but not previous_fact
        """
        if previous_fact is None:
            previous_ids = set()
        else:
            previous_ids = {d.dimension_id for d in previous_fact.dimensions}

        new_ids = {d.dimension_id for d in new_fact.dimensions}

        return {
            "removed_dimension_ids": previous_ids - new_ids,
            "added_dimension_ids": new_ids - previous_ids,
        }

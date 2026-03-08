from src.lunch.base_classes.transformer import Transformer


class DimensionComparer(Transformer):
    """Static methods to compare two dimension dictionaries"""

    @staticmethod
    def compare(previous_dimension: dict | None, new_dimension: dict) -> dict:
        """
        Create a report detailing the differences between two dimensions.
        If previous_dimension is None (first write), no attribute removals are possible.

        Returns a dict with:
          - removed_attribute_ids: attribute id_ values present in previous_dimension but not new_dimension
          - added_attribute_ids:   attribute id_ values present in new_dimension but not previous_dimension
        """
        if previous_dimension is None:
            previous_ids = set()
        else:
            previous_ids = {a["id_"] for a in (previous_dimension.get("attributes") or []) if "id_" in a}

        new_ids = {a["id_"] for a in (new_dimension.get("attributes") or []) if "id_" in a}

        return {
            "removed_attribute_ids": previous_ids - new_ids,
            "added_attribute_ids": new_ids - previous_ids,
        }

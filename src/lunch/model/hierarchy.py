from src.lunch.base_classes.data import Data


class Hierarchy(Data):
    """A hierarchy defines a parent-child ordering of dimension members.

    For example a Time dimension might expose a Year → Quarter → Month
    hierarchy, stored as a list of (parent_member_id, child_member_id) pairs.

    This is the schema-level descriptor.  The actual parent-child pairs are
    stored in ``HierarchyDataStore``.
    """

    def __init__(self, dimension_id: int, name: str):
        self.dimension_id = dimension_id
        self.name = name

    def __eq__(self, other):
        if not isinstance(other, Hierarchy):
            return NotImplemented
        return self.dimension_id == other.dimension_id and self.name == other.name

    def __repr__(self):
        return f"Hierarchy(dimension_id={self.dimension_id!r}, name={self.name!r})"

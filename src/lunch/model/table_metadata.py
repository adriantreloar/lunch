from pyrsistent import PClass, PRecord, CheckedPMap, CheckedPVector, field, pmap_field, pvector_field
from src.lunch.base_classes.transformer import Transformer
from typing import Any
from numpy import dtype

from src.lunch.model.fact import Fact

class TableMetadata(PClass):
    column_names = pvector_field(item_type=str,
                                optional=False,
                                )

    column_types = pvector_field(item_type=dtype,
                                optional=False,
                                 )

    length = field(type=int,
                   mandatory=True)

    #memory_usage = field(type=str,
    #                     mandatory=True)

    @property
    def length_or_none(self) -> int | None:
        if self.length < 0:
            return None
        else:
            return self.length

    # @property
    # def memory_usage_or_none(self) -> str | None:
    #     if not self.memory_usage:
    #         return None
    #     else:
    #         return self.memory_usage

class TableMetadataTransformer(Transformer):
    """Static methods to alter a table definition (e.g. csv input table)"""

    pass
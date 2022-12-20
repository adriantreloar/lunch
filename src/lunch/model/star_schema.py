from pyrsistent import PClass, PRecord, CheckedPMap, CheckedPVector, field, pmap_field
from src.lunch.base_classes.transformer import Transformer
from typing import Any

from src.lunch.model.fact import Fact

class StarSchema(PClass):
    fact = field(type=Fact,
                 mandatory=True,
                 )

    # dimensions by id
    dimensions = pmap_field(int, dict)

class StarSchemaTransformer(Transformer):
    """Static methods to alter a star schema model"""

    @staticmethod
    def get_name(star_schema: StarSchema) -> str:
        return star_schema.fact.name
from pyrsistent import PClass, PRecord, CheckedPMap, CheckedPVector, field, pmap_field
from src.lunch.base_classes.transformer import Transformer
from typing import Any

from src.lunch.model.fact import Fact, FactTransformer

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

    @staticmethod
    def to_dict(star_schema: StarSchema | None) -> dict:
        if star_schema is None:
            return None
        return star_schema.serialize()

    @staticmethod
    def from_dict(star_schema_dict: dict | None) -> StarSchema:
        if star_schema_dict is None:
            return None

        fact_dict = star_schema_dict["fact"]
        dimensions = star_schema_dict["dimensions"]
        if dimensions:
            dimensions = {int(k): v for k, v in star_schema_dict["dimensions"].items()}

        return StarSchema(
            fact=FactTransformer.from_dict(fact_dict),
            dimensions=dimensions
        )

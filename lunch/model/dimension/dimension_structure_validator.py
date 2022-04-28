from lunch.base_classes.transformer import Transformer
from lunch.errors.validation_errors import DimensionValidationError


class DimensionStructureValidator(Transformer):
    """Static methods to check the internal structure of dimension dictionaries"""

    @staticmethod
    def validate(data: dict):
        """ """
        try:
            name = data["name"]
        except KeyError:
            raise DimensionValidationError("No name key")

        if not isinstance(name, str):
            raise DimensionValidationError(f"Name {name} is type {type(name)} whereas it should be str")

        if id_ := data.get("id_"):
            if id_ is None or isinstance(id_, int):
                pass
            else:
                raise DimensionValidationError(f"id_ {id_} is type {type(id_)} whereas it should be int, None, or the key should be left out altogether")

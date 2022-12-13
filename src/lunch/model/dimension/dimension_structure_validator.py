from src.lunch.base_classes.transformer import Transformer
from src.lunch.errors.validation_errors import DimensionValidationError


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
            raise DimensionValidationError(
                f"Name {name} is type {type(name)} whereas it should be str"
            )

        id_ = data.get("id_")
        if id_ is None or isinstance(id_, int):
            pass
        else:
            raise DimensionValidationError(
                f"id_ {id_} is type {type(id_)} whereas it should be int, None, "
                f"or the key should be left out altogether"
            )

        attributes = data.get("attributes")
        if attributes is None:
            pass
        elif isinstance(attributes, list):
            pass
        else:
            raise DimensionValidationError(
                f"attributes {attributes} is type {type(attributes)} whereas it should be list or None, "
                f"or the key should be left out altogether"
            )

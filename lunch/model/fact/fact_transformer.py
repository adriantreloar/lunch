from lunch.base_classes.transformer import Transformer


class FactTransformer(Transformer):
    """Static methods to get fact information from raw fact dictionary
    """

    @staticmethod
    def get_id_from_fact(fact: dict) -> int:
        """

        :param fact: A metadata fact dictionary
        :return: integer id
        """
        return fact["id_"]

    @staticmethod
    def add_id_to_fact(fact: dict, id_: int) -> dict:
        out_fact = fact.copy()
        out_fact["id_"] = id_
        return out_fact

    @staticmethod
    def get_name_from_fact(fact: dict) -> str:
        """

        :param fact: A metadata fact dictionary
        :return: name
        """
        return fact["name"]

    @staticmethod
    def add_name_to_fact(fact: dict, name: str) -> dict:
        out_fact = fact.copy()
        out_fact["name"] = name
        return out_fact

    @staticmethod
    def get_model_version_from_fact(fact: dict) -> int:
        """

        :param fact: A metadata dimension dictionary
        :return: name
        """
        return fact["model_version"]

    @staticmethod
    def add_model_version_to_fact(fact: dict, model_version: int) -> dict:
        out_fact = fact.copy()
        out_fact["model_version"] = model_version
        return out_fact

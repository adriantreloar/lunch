from lunch.base_classes.transformer import Transformer


class DimensionTransformer(Transformer):
    """Static methods to get dimension information from raw dictionary"""

    # These functions don't belong here - they are for transforming the Elements on a dimension,
    # not the dimension (model) itself

    # @staticmethod
    # def get_element_ids_by_name(element_names, dimension_number, dimension_data):
    #    raise NotImplementedError()

    # @staticmethod
    # def get_element_formulas(element_ids, dimension_number, dimension_data):
    #    return {el_id: None for el_id in element_ids}

    # @staticmethod
    # def get_element_names(element_ids, dimension_number, dimension_data):
    #    return {el_id: None for el_id in element_ids}

    # @staticmethod
    # def parse_element_ids(element_ids, dimension_number, dimension_data, parser):
    #    """Parse each element in the list of ids, and then parse the elements in the formulae of those elements
    #    Pass in the parser object. We do it this way (rather than passing the data to the parser),
    #    because we assume the reference data classes know more about the structure of the data than the parser class.
    #    return:
    #        an element_id: parse tree dictionary
    #    """
    #    pass

    @staticmethod
    def get_id_from_dimension(dimension: dict) -> int:
        """

        :param dimension: A metadata dimension dictionary
        :return: integer id
        """
        return dimension["id_"]

    @staticmethod
    def add_id_to_dimension(dimension: dict, id_: int) -> dict:
        out_dimension = dimension.copy()
        out_dimension["id_"] = id_
        return out_dimension

    @staticmethod
    def get_name_from_dimension(dimension: dict) -> str:
        """

        :param dimension: A metadata dimension dictionary
        :return: name
        """
        return dimension["name"]

    @staticmethod
    def add_name_to_dimension(dimension: dict, name: str) -> dict:
        out_dimension = dimension.copy()
        out_dimension["name"] = name
        return out_dimension

    @staticmethod
    def get_model_version_from_dimension(dimension: dict) -> int:
        """

        :param dimension: A metadata dimension dictionary
        :return: name
        """
        return dimension["model_version"]

    @staticmethod
    def add_model_version_to_dimension(dimension: dict, model_version: int) -> dict:
        out_dimension = dimension.copy()
        out_dimension["model_version"] = model_version
        return out_dimension

    @staticmethod
    def add_attribute_ids_to_dimension(dimension: dict) -> dict:
        out_dimension = dimension.copy()
        if attributes := out_dimension.get("attributes"):
            max_id = max([attribute.get("id_", 0) for attribute in attributes])
            for attribute in attributes:
                if "id_" not in attribute or not attribute["id_"]:
                    attribute["id_"] = max_id + 1
                    max_id += 1

        return out_dimension

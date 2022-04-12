class DimensionTransformer:
    """ Static methods to get dimension information from raw data

    The dimension_data is given to these functions at a given version
    """

    @staticmethod
    def get_element_ids_by_name(element_names, dimension_number, dimension_data):
        raise NotImplementedError()

    @staticmethod
    def get_element_formulas(element_ids, dimension_number, dimension_data):
        return {el_id: None for el_id in element_ids}

    @staticmethod
    def get_element_names(element_ids, dimension_number, dimension_data):
        return {el_id: None for el_id in element_ids}

    @staticmethod
    def parse_element_ids(element_ids, dimension_number, dimension_data, parser):
        """ Parse each element in the list of ids, and then parse the elements in the formulae of those elements
        Pass in the parser object. We do it this way (rather than passing the data to the parser),
        because we assume the reference data classes know more about the structure of the data than the parser class.
        return:
            an element_id: parse tree dictionary
        """

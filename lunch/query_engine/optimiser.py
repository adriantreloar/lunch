from lunch.base_classes.transformer import Transformer

class Optimiser(Transformer):
    """Each engine will have its own optimiser.
    Each optimiser knows how to turn generic parse results into particular instructions
    that its engine will understand.
    """

    def create_query_instructions(self, v_id, full_parse_result, statistics):
        """Take a full parse result which contains parsed element formulae (at a given version)
        and which has been checked against a model at the same version

        The statistics will be specific to the engine that this optimiser is created for.
        """
        return {}

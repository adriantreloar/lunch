from lunch.base_classes.conductor import Conductor


class Store(Conductor):
    """ Base storage class

    Storage manages a cache and persistence
    """

    def persist(self, data):
        """ Do we want a separate serialiser?

        Some persistence (e.g. parquet) needs to know file locations etc.

        We can't just serialise and then save to file
        """

        # Who knows what we return - informaton for an index, or simply True on success
        return False
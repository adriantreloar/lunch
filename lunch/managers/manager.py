from lunch.base_classes.conductor import Conductor


class Manager(Conductor):
    """Base class for Managers.

    When a server gets asked to do something, it will route the request to the appropriate Manager.

    The manager will then ensure that the request is handled, and the result passed back to the server.
    """

    pass

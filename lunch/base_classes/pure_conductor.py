

class PureConductor():
    """ A base class, for classes which merely ask other classes to do various tasks.

    PureConductors hold no state, even references to stateful classes.
    PureConductors do no transformations, they just make calls to other classes.
    PureConductors have only static functions.

    Tests for conductors should only need to test that appropriate (mock) calls have been made,
    and the results or errors from those calls handled and routed appropriately.
    """
    pass
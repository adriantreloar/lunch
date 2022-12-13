class Conductor:
    """A base class, for classes which merely ask other classes to do various tasks.

    Conductors hold no state except for references to the classes they conduct.
    Conductors do no transformations, they just make calls to other classes.
    Conductors have only static functions.

    Tests for conductors should only need to test that appropriate (mock) calls have been made,
    and the results or errors from those calls handled and routed appropriately.
    """

    pass

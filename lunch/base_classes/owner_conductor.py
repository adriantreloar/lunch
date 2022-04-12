
class OwnerConductor():
    """ A base class, for classes which merely ask the classes they were initialized with to do various tasks.

    A normal Conductor holds no state, even references to stateful classes.
    A normal Conductor has only static functions.

    Calling a normal conductor can be tedious, since we have to pass in a long (and correct) list of objects
    to the static functions.

    Thus we have the convenience class - the OwnerConductor

    An OwnerConductor is initialized with the objects it will call, and the resulting method calls are simpler
    and more convenient to use.

    In order to keep architectural purity, each OwnerConductor is initialized with its owned classes,
    which are then passed to the static functions of the matching PureConductor class.

    e.g.

    class MyOwnerConductor(OwnerConductor):

        def __init__(self, some_transformer, some_stateful):
            self.some_transformer = some_transformer
            self.some_stateful = some_stateful

        def do_the_moog():
            # Call the matching static function, with the appropriate arguments
            return SomePureConductor.do_the_moog(self.some_transformer, self.some_stateful)
    """
    pass
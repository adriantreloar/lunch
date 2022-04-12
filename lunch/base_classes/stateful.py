
class Stateful():
    """ A base class, for classes which hold state, such as caches.

    Statefuls may call other classes to do transformations, but must not do any transformations.
    Statefuls should hold state.

    Tests for statefuls can be complex, as they have to test before and after scenarios, and may need to be tested
    for asynchronous correctness.

    By limiting the number of Statefuls, we avoid Object Oriented Programming, which mixes state and function.
    """
    pass
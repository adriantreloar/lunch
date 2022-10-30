class Constant:
    """A base class, for classes which hold constant data.

    Constants hold unchanging state

    Tests for constants should only need to test that any functions that pull out part of a constant
    return the correct part.
    e.g.
    >>> assert my_constant.constant_tree.leaves == ['leaf1', 'leaf2']
    """

    pass

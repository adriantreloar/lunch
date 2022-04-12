
class Transformer():
    """ A base class, for classes which transform data.

    Transformers should contain only static functions.
    Transformers are stateless.

    If a Transformer needs a cache to speed up its transformation, create a Conductor class that:
        tries the cache (a Stateful class)
        calls the Transformer if the cache is empty.

    Tests for transformers are very simple. Test that input data is transformed to correct output data.

    e.g. assert Transformer.reverse("foo") == "oof"
    """
    pass
Queries
=======

A **query** is a pure-data request for information.  All query classes are
``Data`` subclasses — they carry no behaviour and can be inspected, logged, or
passed between components without side effects.

``Query`` is the intermediate base class that marks an object as a query.
Concrete query types inherit from ``Query``.

.. contents:: On this page
   :local:
   :depth: 2


Query classes
-------------

Query
~~~~~

*Module:* ``src.lunch.queries.query``

Abstract base marker class.  Inherits ``Data``.  Carries no fields.

.. code-block:: python

    class Query(Data):
        pass


CubeQuery
~~~~~~~~~

*Module:* ``src.lunch.queries.cube_query``

The initial vague request from a caller.  Fields such as ``version`` and
``projection`` may use shorthand values like ``"latest"`` or ``"default"``.

.. code-block:: python

    CubeQuery(
        star_schema_name: str,
        version: Any,        # e.g. "latest" or a concrete int
        projection: Any,     # e.g. "default" or None
        filter: Optional[Any],
        aggregation: Any,    # e.g. "default" or None
    )

**Example:**

.. code-block:: python

    CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )


FullySpecifiedFactQuery
~~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.queries.fully_specified_fact_query``

The output of the QuerySpecifier when given a ``CubeQuery``.  All fields are
resolved to concrete values.  This is the input to the QueryPlanner.

.. code-block:: python

    FullySpecifiedFactQuery(
        star_schema: StarSchema,
        version: Version,
        dimensions: list[dict],
        measures: list[int],
        filters: list[Any],
        aggregations: list[Any],
    )

``dimensions`` is a list of dimension metadata dictionaries (the same
``dict`` format used throughout the storage layer).  ``measures`` is a list
of integer measure ids.  ``filters`` and ``aggregations`` are lists of
predicate / function descriptors whose exact types are determined by the
query engine implementation.


Source locations
----------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Class
     - Source path
   * - ``Query``
     - ``src/lunch/queries/query.py``
   * - ``CubeQuery``
     - ``src/lunch/queries/cube_query.py``
   * - ``FullySpecifiedFactQuery``
     - ``src/lunch/queries/fully_specified_fact_query.py``

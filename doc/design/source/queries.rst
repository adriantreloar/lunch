Queries
=======

.. note::

   **Partially implemented.**

   ``Query``, ``CubeQuery``, and ``FullySpecifiedFactQuery`` are implemented
   in ``src/lunch/queries/``.  See the reference documentation for details.
   The QuerySpecifier, QueryPlanner, QueryEnactor, and QueryEngine components
   described below are not yet implemented.

A **query** is a request for information.  Queries start out intentionally
vague — a caller asks for a high-level result without specifying how that
result should be retrieved or computed.  Query engines call a QuerySpecifier to
progressively refining vague queries into concrete plans.

.. contents:: On this page
   :local:
   :depth: 2


CubeQuery — an example of a vague query
-----------------------------------------

Queries begin at the highest level of abstraction.  A ``CubeQuery`` might look
like this:

.. code-block:: python

    CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )

This says: *"Give me the latest version of the Sales cube, using the default
projection and the default aggregation. Do not filter the data"*  It says nothing about which
dimensions to scan, which partitions to read, or how to parallelise the work
— those details are for QueryEngine to determine (by asking the Planner, which it owns).


How query engines refine queries
----------------------------------

A query engine receives a vague query and ultimately returns data to the
caller.  It does this in three stages, each handled by a dedicated
``Conductor``:

Specify
~~~~~~~

The **QuerySpecifier** resolves every vague field in the query against the
Fact Schema (via the ``VersionManager`` and ``ModelManager``) and returns a
``FullySpecifiedFactQuery`` — a query in which every field is concrete and
unambiguous.

Plan
~~~~

The **Planner** inspects the ``FullySpecifiedFactQuery``, determines which
storage partitions and columns must be read, and constructs a **DAG Plan** —
a directed acyclic graph of ``BasicPlan`` nodes connected by data-dependency
edges.  Steps that are independent of each other are left unconnected and can
be executed concurrently.  Steps that depend on the output of earlier steps
are connected by edges.

Enact
~~~~~

The **Enactor** walks the DAG Plan, executing nodes as their inputs become
available, and returns the resulting data to the ``QueryEngine``.

The flow from vague query to returned data looks like this:

.. code-block:: text

    CubeQuery (vague)
        │
        ▼
    QueryEngine (Conductor)
        │
        ├─► QuerySpecifier ──► FullySpecifiedFactQuery
        │
        ├─► Planner        ──► DAG Plan
        │
        └─► Enactor        ──► data ──► returned to caller


QuerySpecifier responsibilities
------------------------------------

For all queries, the QuerySpecifier:

1. Selects the set of QueryTransformer relevant to that query type.
2. Passes the query to each QueryTransformer.
3. Each QueryTransformer returns the part of the query it owns — as a
   refined sub-query.
4. The QuerySpecifier then puts all of the usb-query parts together to form a completely specified query
5. The completely specified query is returned to the QueryEngine

This design keeps each transformer focused on a single concern, and keeps
query engines as thin aggregators rather than monolithic planners.


Queries as Data
---------------

All query objects are ``Data`` subclasses — pure data containers with no
behaviour.  This means a query can be inspected, logged, or passed between
components without side effects.

``Query`` is the intermediate base class that sits between ``Data`` and all
concrete query types.  It carries no behaviour of its own; its sole purpose
is to mark an object as a query.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Query type
     - Base class
     - Role
   * - ``Query``
     - ``Data``
     - Abstract base for all query types.
   * - ``CubeQuery``
     - ``Query``
     - The initial vague request from the caller.  Fields such as ``version``
       and ``projection`` may use shorthand values like ``"latest"`` or
       ``"default"``.
   * - ``FullySpecifiedFactQuery``
     - ``Query``
     - The output of the :doc:`query_specifier` when given a CubeQuery.  All fields are resolved to
       concrete values: a ``StarSchema`` object, a concrete ``Version``, an
       explicit list of ``Dimension`` objects, measure ids, filters, and
       aggregation functions.  This is the input to the :doc:`query_planner`.


Relationship to Plans
----------------------

The output of the planning stage is a **DAG Plan** (``DagPlan``) — a directed
acyclic graph of ``BasicPlan`` nodes.  A ``DagPlan`` is a ``Data`` object: it
carries no behaviour and can be inspected, logged, or passed between
components without side effects.

The ``DagPlan`` replaces ``SerialPlan`` and ``ParallelPlan`` for query
execution.  Both of those structures are special cases of a DAG:

* A ``SerialPlan`` is a DAG that is a simple chain.
* A ``ParallelPlan`` is a DAG with no edges (all nodes independent).

See :doc:`query_planner` for details on ``DagPlan`` fields and how the
:doc:`query_planner` constructs them.  See :doc:`query_enactor` for how the
:doc:`query_enactor` executes them.


Suggested source locations
--------------------------

When these classes are implemented they should live under a ``queries``
package inside ``src/lunch/``:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Class
     - Suggested path
   * - ``Query``
     - ``src/lunch/queries/query.py``
   * - ``CubeQuery``
     - ``src/lunch/queries/cube_query.py``
   * - ``FullySpecifiedFactQuery``
     - ``src/lunch/queries/fully_specified_fact_query.py``
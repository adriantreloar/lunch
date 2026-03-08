Query Enactor
=============

.. note::

   **Implemented.**  See ``src/lunch/query_engines/query_enactor.py`` and
   ``src/lunch/query_engines/cube_query_enactor.py``.

A **QueryEnactor** is a ``Conductor`` that receives a DAG Plan (produced by the
:doc:`query_planner`) and executes it — fetching, filtering, joining, and
aggregating data from storage — to produce the data that the
:doc:`query_engines` returns to the caller.

The ``QueryEnactor`` is the final stage of the query pipeline.

.. contents:: On this page
   :local:
   :depth: 2


Responsibilities
----------------

A ``QueryEnactor``:

* Walks the DAG Plan, starting from nodes whose inputs are all satisfied.
* Dispatches each ``BasicPlan`` node to the appropriate handler based on the
  node's name.
* Passes output data (keyed by UUID) from completed nodes to the inputs of
  dependent nodes.
* Executes nodes that have no unsatisfied dependencies concurrently.
* Collects the outputs identified by the DAG Plan's ``outputs`` field and
  returns them to the ``QueryEngine``.

The ``QueryEnactor`` is a ``Conductor``: it holds references to the relevant
data stores (``FactDataStore``, ``DimensionDataStore``) and delegates all
data-manipulation logic to ``Transformer`` helpers.  It performs no
transformations itself.


Execution model
---------------

The enactor maintains a **result registry** — a mapping from output UUID to
the data produced for that UUID.

It processes the DAG as follows:

1. Identify all nodes whose input UUIDs are all present in the result registry
   (initially: nodes with no inputs).
2. Execute those nodes concurrently (``asyncio.gather`` or equivalent).
3. On completion, add each node's outputs to the result registry.
4. Repeat from step 1 until all nodes have been executed.
5. Return the data stored under the DAG Plan's ``outputs`` UUIDs.

This loop respects the dependency order expressed by the DAG edges without
requiring the enactor to inspect the edge set directly — the UUID-based
registry provides a natural barrier.


Step dispatch
-------------

The enactor maintains a dispatch table mapping step names to handler coroutines.
Each handler is a module-level ``async`` function (following the ``Conductor``
pattern of thin delegation):

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Step name
     - Handler responsibility
   * - ``FetchDimensionData``
     - Read the specified dimension columns from ``DimensionDataStore`` at the
       given version.
   * - ``FetchFactData``
     - Read the specified fact partition columns from ``FactDataStore`` at the
       given version.
   * - ``JoinDimensionsToFact``
     - Perform an in-memory join of dimension data onto the fact table, keyed by
       dimension member ids.
   * - ``Aggregate``
     - Apply the specified aggregation function (e.g. ``sum``, ``count``) to the
       joined dataset.

New step types can be added by registering additional entries in the dispatch
table; no changes to the enactor's control-flow loop are required.


Classes
-------

QueryEnactor
~~~~~~~~~~~~

**Role:** ``Conductor``

Abstract base for all query enactors.  Defines the ``enact`` interface:

.. code-block:: python

    async def enact(self, plan: DagPlan) -> QueryResult:
        ...

Subclasses inject the appropriate data stores and ``Transformer`` helpers via
their constructors.

**Suggested source location:**
``src/lunch/query_engines/query_enactor.py``

CubeQueryEnactor
~~~~~~~~~~~~~~~~

**Role:** ``Conductor``

Concrete enactor for cube queries.  Holds references to a ``FactDataStore``
and a ``DimensionDataStore``, and registers handlers for all step types
produced by the :doc:`query_planner`.

**Suggested source location:**
``src/lunch/query_engines/cube_query_enactor.py``

QueryResult
~~~~~~~~~~~

**Role:** ``Data``

Pure data container returned by the enactor to the ``QueryEngine``.  Fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``data``
     - The result dataset (e.g. a pandas ``DataFrame`` or Arrow ``Table``).
   * - ``query``
     - The ``FullySpecifiedFactQuery`` that produced this result.
   * - ``plan``
     - The ``DagPlan`` that was enacted (useful for debugging and profiling).

**Suggested source location:**
``src/lunch/query_engines/query_result.py``

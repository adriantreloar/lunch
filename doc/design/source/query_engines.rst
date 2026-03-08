Query Engines
=============

.. warning::

   **Design document — not yet implemented.**

   The query engine classes and interfaces described on this page do not yet
   exist in the codebase.  This document describes intended design only.

A **query engine** processes a vague query and returns data to the caller.  It
does this in three stages, each handled by a dedicated ``Conductor``:

1. **QuerySpecifier** — resolves a vague query against the Fact Schema and
   produces a ``FullySpecifiedFactQuery``.
2. **Planner** — converts the ``FullySpecifiedFactQuery`` into a DAG Plan that
   describes every retrieval, filter, and combination step needed.
3. **Enactor** — follows the DAG Plan, executing each step to retrieve the
   required data.

The ``QueryEngine`` itself is also a ``Conductor``.  It holds references to all
three components and orchestrates the full pipeline: it passes the vague query
to the ``QuerySpecifier``, hands the result to the ``Planner``, gives the plan
to the ``Enactor``, and returns the retrieved data to the caller.

.. contents:: On this page
   :local:
   :depth: 2


Pipeline overview
-----------------

.. code-block:: text

    Caller
      │  vague CubeQuery
      ▼
    QueryEngine (Conductor)
      │
      ├─► QuerySpecifier (Conductor)
      │     uses: VersionManager, ModelManager
      │     in:   CubeQuery (vague)
      │     out:  FullySpecifiedFactQuery
      │
      ├─► Planner (Conductor)
      │     in:   FullySpecifiedFactQuery
      │     out:  DAG Plan
      │
      ├─► Enactor (Conductor)
      │     in:   DAG Plan
      │     out:  retrieved data
      │
      └─► Caller receives data


CubeQueryEngine — a skeleton example
--------------------------------------

This example traces how a ``CubeQueryEngine`` processes the following vague
query:

.. code-block:: python

    CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        aggregation="default",
    )

Step 1 — QuerySpecifier resolves the query
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` delegates to its ``QuerySpecifier``, which contacts the
``VersionManager`` and ``ModelManager`` to resolve every vague field:

.. code-block:: python

    fully_specified_query = await query_specifier.specify(
        query=vague_query,
    )
    # Returns a FullySpecifiedFactQuery with:
    #   star_schema=<StarSchema: Sales>
    #   version=Version(version=7, ...)
    #   dimensions=[<Dimension: Region>, <Dimension: Product>, ...]
    #   measures=[<Measure: Revenue>, <Measure: Units>]
    #   filters=[]
    #   aggregations=["sum"]

Step 2 — Planner creates a DAG Plan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` passes the ``FullySpecifiedFactQuery`` to its
``Planner``:

.. code-block:: python

    plan = await planner.plan(fully_specified_query)
    # Returns a DAG Plan whose nodes are BasicPlan steps and whose edges
    # express which steps must complete before others may start.

Step 3 — Enactor follows the plan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` passes the DAG Plan to its ``Enactor``:

.. code-block:: python

    data = await enactor.enact(plan)

Step 4 — QueryEngine returns the data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    return data


The DAG Plan
------------

Plans are now expressed as a **directed acyclic graph (DAG)** rather than as
``SerialPlan`` or ``ParallelPlan`` sequences.  The DAG is strictly more
expressive: a linear chain of nodes is a serial plan; a set of nodes with no
edges between them is a parallel plan; and a mixed structure captures any
combination.

Each **node** in the DAG is a ``BasicPlan`` — a named step with typed inputs
and outputs.  **Edges** express dependency: an edge from node A to node B means
B may not start until A has completed and its outputs are available.

Inputs and outputs are identified by **UUIDs**, exactly as in the existing
``BasicPlan`` / ``RemotePlan`` design.  A UUID that appears in the outputs of
one node and the inputs of another node is the mechanism by which data flows
along an edge.

Steps whose input UUIDs are all satisfied (i.e. all predecessor nodes have
completed) may be started immediately.  Steps with no predecessors may run
from the outset and can be executed concurrently.

.. code-block:: text

    ┌────────────────────┐     ┌────────────────────┐
    │  FetchRegionData   │     │  FetchProductData  │
    │  out: uuid-A       │     │  out: uuid-B       │
    └────────┬───────────┘     └──────────┬─────────┘
             │                            │
             └──────────┬─────────────────┘
                        ▼
             ┌────────────────────┐
             │  JoinAndAggregate  │
             │  in:  uuid-A, B   │
             │  out: uuid-C      │
             └────────────────────┘

The ``SerialPlan`` and ``ParallelPlan`` types from the import pipeline are
**not used** by the query engine.  The DAG Plan is the single plan type for
queries.


Classes
-------

QueryEngine
~~~~~~~~~~~

**Role:** ``Conductor``

``QueryEngine`` is the top-level entry point for queries.  It holds references
to a ``QuerySpecifier``, a ``Planner``, and an ``Enactor``, and orchestrates
the full pipeline: specification, planning, enactment, and returning data to the
caller.  It performs no data transformations itself.

**Suggested source location:** ``src/lunch/query_engines/query_engine.py``

See :doc:`query_specifier`, :doc:`query_planner`, and :doc:`query_enactor` for
details on each component.


CubeQueryEngine
~~~~~~~~~~~~~~~

**Role:** ``Conductor`` (subclass of ``QueryEngine``)

``CubeQueryEngine`` is the concrete ``QueryEngine`` for cube queries.  It wires
together a ``CubeQuerySpecifier``, a ``CubeQueryPlanner``, and a
``CubeQueryEnactor`` and exposes a ``query`` method that accepts a ``CubeQuery``
and returns data.

**Suggested source location:** ``src/lunch/query_engines/cube_query_engine.py``

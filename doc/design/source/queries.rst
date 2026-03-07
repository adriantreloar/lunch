Queries
=======

.. warning::

   **Design document — not yet implemented.**

   The query classes and interfaces described on this page do not yet exist
   in the codebase.  This document describes intended design only.

A **query** is a request for information.  Queries start out intentionally
vague — a caller asks for a high-level result without specifying how that
result should be retrieved or computed.  Query engines are responsible for
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
        aggregation="default",
    )

This says: *"Give me the latest version of the Sales cube, using the default
projection and the default aggregation."*  It says nothing about which
dimensions to scan, which partitions to read, or how to parallelise the work
— those details are for query engines to determine.


How query engines refine queries
----------------------------------

A query engine receives a query and ultimately returns a **plan**.  The root
query engine always returns a plan — compound queries may be returned as
intermediate steps by sub-engines, but plans are returned at the leaf steps,
where a query is simple enough to be transformed directly into storage
instructions.  For anything other than the simplest queries, the root engine
does this in two stages:

Decompose
~~~~~~~~~

The engine decides which *query transformers* should handle different aspects
of the query.  Each transformer is handed the full query and extracts the
slice it is responsible for.

Recurse and aggregate
~~~~~~~~~~~~~~~~~~~~~

Each transformer either:

* Returns a **finer-grained sub-query**, which is handed to the appropriate
  engine for the next refinement round, or
* Transforms the query slice directly into a **Plan** (for leaf-level,
  maximally-refined queries).

As plans come back up the call stack, each engine **aggregates** the returned
plans into a single, larger plan and returns that combined plan to its caller.

Once the original top-level query engine has gathered all of the plans
returned by its transformers, it returns one overall plan.  That plan is then
enacted by an ``Enactor`` — just as import plans are enacted by
``DimensionImportEnactor`` or ``FactImportEnactor``.

The flow from vague query to enacted plan looks like this:

.. code-block:: text

    CubeQuery (vague)
        │
        ▼
    QueryEngine (top level)
        ├── Transformer A  ──► sub-query ──► QueryEngine (next level)
        │                                       ├── Transformer C ──► Plan
        │                                       └── Transformer D ──► Plan
        │                                       └── aggregated Plan ◄──
        ├── Transformer B  ──► Plan
        └── aggregated overall Plan ◄──────────────────────────────────
                │
                ▼
            Enactor  (executes the plan)


Query transformer responsibilities
------------------------------------

For a **complex query**, the query engine:

1. Selects the set of transformers relevant to that query type.
2. Passes the query to each transformer.
3. Each transformer returns the part of the query it owns — either as a
   refined sub-query or directly as a ``Plan``.

For the **simplest (leaf-level) queries**, every transformer converts its
slice of the query directly into a ``Plan`` with no further recursion.

This design keeps each transformer focused on a single concern, and keeps
query engines as thin aggregators rather than monolithic planners.


Queries as Data
---------------

Like plans, all query objects are ``Data`` subclasses — pure data containers
with no behaviour.  This means a query can be inspected, logged, or passed
between components without side effects.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Query type
     - Role
   * - ``CubeQuery``
     - The initial vague request from the caller.
   * - ``ResolvedCubeQuery``
     - An enriched query produced by a Transformer; carries an explicit
       ``Version``, ``StarSchema``, projection, and aggregation.
   * - ``BasicQuery``
     - A single named query step with typed inputs and outputs (mirrors
       ``BasicPlan``).
   * - ``SerialQuery``
     - An ordered sequence of ``BasicQuery`` steps where later steps may
       consume the outputs of earlier ones via UUID references (mirrors
       ``SerialPlan``).


Relationship to Plans
----------------------

Plans (``BasicPlan``, ``SerialPlan``, ``ParallelPlan``, ``RemotePlan``) are
the output of the query-refinement process.  A plan carries no behaviour of
its own — it is a pure-data description of work to be done.  See the
Reference documentation (``doc/reference/``) for details on plan types and
how enactors dispatch on them.
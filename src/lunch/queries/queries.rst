Queries
=======

A **query** is a request for information.  Queries start out intentionally
vague — a caller asks for a high-level result without specifying how that
result should be retrieved or computed.  Query engines are responsible for
progressively refining vague queries into concrete plans.


CubeQuery — an example of a vague query
-----------------------------------------

.. code-block:: python

    CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        aggregation="default",
    )

This query says: *"Give me the latest version of the Sales cube, using the
default projection and the default aggregation."*  It says nothing about which
dimensions to scan, which partitions to read, or how to parallelise the work.
That is intentional — it is the query engine's job to figure those things out.


How query engines refine queries
----------------------------------

A query engine receives a query and turns it into a **plan**.  For anything
other than the simplest query, it does this in two stages:

1. **Decompose** — the engine decides which *query transformers* should handle
   different aspects of the query.  Each transformer is handed the full query
   and extracts the slice that it is responsible for.

2. **Recurse** — each transformer either returns a finer-grained sub-query
   (which is handed to the appropriate engine for the next refinement round),
   or — for the simplest, leaf-level queries — transforms the query directly
   into a ``Plan``.

As sub-queries are resolved and plans come back up the call stack, each engine
**aggregates** the returned plans into a single, larger plan and returns that
combined plan to its caller.

Once the original (top-level) query engine has gathered all of the plans
returned by its transformers, it returns one overall plan.  That plan is then
enacted by an ``Enactor`` at some point — just as import plans are enacted by
``DimensionImportEnactor`` or ``FactImportEnactor``.


Query transformer responsibilities
------------------------------------

For a complex query, the query engine:

* Selects the set of transformers relevant to that query type.
* Passes the query to each transformer.
* Each transformer returns the part of the query it owns — either as a
  refined sub-query or directly as a ``Plan``.

For the simplest (leaf-level) queries, every transformer converts its slice of
the query into a ``Plan`` directly, with no further recursion.


Relationship to Plans
----------------------

Plans (``BasicPlan``, ``SerialPlan``, ``ParallelPlan``, ``RemotePlan``) are
the output of the query-refinement process.  A plan carries no behaviour of
its own — it is a pure-data description of work to be done.  See the
*Plans* documentation for details on plan types and how enactors dispatch on
them.

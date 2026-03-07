Query Engines
=============

.. warning::

   **Design document — not yet implemented.**

   The query engine classes and interfaces described on this page do not yet
   exist in the codebase.  This document describes intended design only.

A query engine does the following:

Takes a query.

Turns the query into a plan.

A plan is a number of (sub)queries and a combination step.
Each (sub)query will have a planned engine.

The subqueries are performed.
The combination step combines the subqueries to return a result.
Returns the subqueries with statistics.
Returns the combined results.


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

Step 1 — gather context from managers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The engine is a ``Conductor``.  It holds references to a ``VersionManager``
and a ``ModelManager`` and uses them to resolve the vague fields in the query
into explicit, concrete values:

.. code-block:: python

    # Resolve "latest" to a concrete Version object
    current_version = await version_manager.get_current_version()

    # Retrieve the StarSchema at that version
    star_schema = await model_manager.get_star_schema(
        name=query.star_schema_name,
        version=current_version,
    )

Step 2 — enrich the query with a Transformer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The engine passes the original query together with the retrieved context to
``CubeQueryContextResolver``, a ``Transformer`` whose sole job is to produce
an enriched, explicit query:

.. code-block:: python

    resolved_query = CubeQueryContextResolver.resolve(
        query=query,
        version=current_version,
        star_schema=star_schema,
    )
    # resolved_query is now a ResolvedCubeQuery:
    #   ResolvedCubeQuery(
    #       star_schema=<StarSchema: Sales>,
    #       version=<Version: 2024-03-07T10:00:00>,
    #       projection=<DefaultProjection>,
    #       aggregation=<DefaultAggregation>,
    #   )

``CubeQueryContextResolver`` is stateless and holds no references — it is a
pure transformation of its inputs.

Step 3 — hand the enriched query to sub-engines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` selects the sub-engines appropriate for the enriched
query and dispatches to each:

.. code-block:: python

    projection_plan = await ProjectionQueryEngine.process(
        resolved_query.as_projection_query()
    )

    aggregation_plan = await AggregationQueryEngine.process(
        resolved_query.as_aggregation_query()
    )

Each sub-engine may itself decompose, enrich, and recurse further before
returning its own plan.

Step 4 — aggregate the returned plans
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once all sub-engine plans have been collected, the engine passes them to
``CubePlanAggregator``, a ``Transformer`` that combines them into a single
plan:

.. code-block:: python

    overall_plan = CubePlanAggregator.aggregate(
        plans=[projection_plan, aggregation_plan],
    )
    # Returns, for example:
    #   SerialPlan(steps=[
    #       <projection_plan>,
    #       <aggregation_plan>,
    #   ])

Step 5 — return the overall plan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` returns ``overall_plan`` to its caller.  At some
point an ``Enactor`` will receive this plan and execute it against storage.

.. code-block:: python

    return overall_plan
A ``QueryEngine`` receives a query — a request for data — and returns a
``Plan`` — a pure-data description of how to retrieve that data from the
storage layer.  See :doc:`queries` for query types and :doc:`plans` for plan
types.

Because a ``QueryEngine`` is a ``Conductor``, it performs no transformations
itself.  All splitting, enriching, and plan-combining is delegated to
``Transformer`` objects.  The engine merely decides which transformers to
apply, which sub-engines to invoke, and which transformer to use when
aggregating the returned plans.

The simple case
~~~~~~~~~~~~~~~~

When a query is simple enough to be resolved directly, a ``Transformer``
converts it into a ``Plan`` and the engine returns that plan immediately.
This is the leaf step — no sub-engines are involved.

The complex case
~~~~~~~~~~~~~~~~~

When a query requires further refinement, the engine:

1. Delegates to a ``Transformer`` to decompose the query into a
   **compound-query** — either a serial or parallel grouping of sub-queries.

   - A **serial compound-query** is list-like: an ordered sequence of
     sub-queries where later steps may consume the outputs of earlier ones
     via UUID references.
   - A **parallel compound-query** is dict-like: a mapping from UUID keys to
     independent sub-queries whose results can be referenced by name.

2. Dispatches each sub-query to a specialised ``QueryEngine``.  Sub-engines
   may themselves return compound-queries for further recursion, or plans at
   their own leaf steps.

3. Collects the plans returned by all sub-engines and delegates to a
   ``Transformer`` to aggregate them into a single overall plan.

4. Returns that overall plan.  The root ``QueryEngine`` always returns a
   plan — compound queries exist only as intermediate steps inside the
   recursion.

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
    #       version=Version(
    #           version=42,
    #           model_version=7,
    #           reference_data_version=15,
    #           cube_data_version=42,
    #           operations_version=3,
    #           website_version=1,
    #       ),
    #       projection=<DefaultProjection>,
    #       aggregation=<DefaultAggregation>,
    #   )

``CubeQueryContextResolver`` is stateless and holds no references — it is a
pure transformation of its inputs. The ``ResolvedCubeQuery`` it returns is a
``Data`` subclass: a pure data container with no behaviour, just like a
``BasicPlan`` or ``SerialPlan``.

Step 3 — decompose the enriched query with a Transformer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because the ``CubeQueryEngine`` is a ``Conductor``, it does not construct the
``SerialQuery`` directly.  Instead it delegates to ``CubeQueryDecomposer``, a
``Transformer`` whose job is to split the enriched query into the appropriate
sequence of sub-queries:

.. code-block:: python

    from uuid import uuid1

    projection_query_id = uuid1()
    aggregation_plan_id = uuid1()

    serial_query = CubeQueryDecomposer.decompose(
        query=resolved_query,
        projection_query_id=projection_query_id,
        aggregation_plan_id=aggregation_plan_id,
    )
    # Returns a SerialQuery whose steps chain two sub-engines:
    #   SerialQuery(steps=[
    #       BasicQuery(
    #           name="_run_projection_query_engine",   # → returns a Query
    #           inputs={"query": resolved_query},
    #           outputs={"projection_query": projection_query_id},
    #       ),
    #       BasicQuery(
    #           name="_run_aggregation_query_engine",  # → returns a Plan (final step)
    #           inputs={"query": resolved_query,
    #                   "projection_query": projection_query_id},
    #           outputs={"aggregation_plan": aggregation_plan_id},
    #       ),
    #   ])

Every step except the last is sent to a ``QueryEngine`` that returns a
**Query** — that returned query is then passed as input to the next step via
its UUID.  The final step is sent to a ``QueryEngine`` that returns a
**Plan**.

The first step asks ``ProjectionQueryEngine`` to refine the query into a
``projection_query`` — a new query whose fields carry the projected columns
and their types.  The second step asks ``AggregationQueryEngine`` to consume
that ``projection_query`` and produce the final aggregation plan, guaranteeing
that the aggregation column types match the projection.

The ``AggregationQueryEngine`` receives both the ``resolved_query`` and the
``projection_query`` from step 1, so it can inspect the projected columns and
their types when constructing the aggregation plan.

Step 4 — aggregate the returned plans
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` passes the plan returned by the final step of
``serial_query`` — along with any plans returned by other sub-engines — to
``CubePlanAggregator``, a ``Transformer`` that combines them into a single
overall plan:

.. code-block:: python

    overall_plan = CubePlanAggregator.aggregate(
        plans=[aggregation_plan],
    )
    # Returns a storage-layer Plan whose steps describe concrete operations
    # that an Enactor can execute.  For example:
    #   SerialPlan(steps=[
    #       BasicPlan('_read_fact_partitions',
    #                 inputs=['fact_id', 'version', 'partition_filter'],
    #                 outputs={'fact_data': <uuid>}),
    #       BasicPlan('_apply_aggregation',
    #                 inputs=['fact_data': <uuid>, 'aggregation_spec'],
    #                 outputs={'result': <uuid>}),
    #   ])

Step 5 — return the overall plan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CubeQueryEngine`` returns ``overall_plan`` to its caller.  At some
point an ``Enactor`` will receive this plan and execute it against storage.

.. code-block:: python

    return overall_plan

A Query Engine does the following:

Takes a query. A query is a request for data.

Turns the query into a plan or into a compound-query which contains a number of serial or parallel sub-queries.
The root Query Engine always returns a plan. Compound queries are intermediate — returned by sub-engines when further refinement is needed. Plans are returned at the leaf steps, where a query is simple enough to be transformed directly into storage instructions.

A plan is instructions that describe how to get that data from the storage layer.

The Query Engine then passes each sub-query in the compound-query to a group of specialised query engines.
For a serial compound-query the group is list-like — an ordered sequence of sub-queries where later steps may depend on the outputs of earlier ones.
For a parallel compound-query the group is dict-like — a mapping from UUID keys to sub-queries, where each UUID identifies an independent sub-query whose result can be referenced by name.

Any returned plans from the specialised query engines are then combined to form a complex-plan.

The Query Engine returns this complex plan.

At the simplest level, when a passed query is simple enough, the query engine will simply transform that query into a plan and return it.

Because a Query Engine is a Conductor, all the Transformations (such as splitting the query up, or transforming the query into a plan) are performed by separate objects.
The Query Engine merely decides which Transformations are applied, which sub Query Engines need to be run on any queries, and which Transformations should be used to combine returned plans.

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

Query Engines
=============

This page documents the classes used by the query engine pipeline: pure
``Data`` subclasses that carry no behaviour, and the ``Conductor`` and
``Transformer`` classes that implement the query specification stage.

.. contents:: On this page
   :local:
   :depth: 2


DagPlan
-------

*Module:* ``src.lunch.query_engines.dag_plan``

A directed acyclic graph (DAG) plan.  Each node is a ``BasicPlan`` step;
edges express dependencies between steps.

.. code-block:: python

    DagPlan(
        nodes: dict[UUID, BasicPlan],
        edges: set[tuple[UUID, UUID]],
        inputs: set[UUID],
        outputs: set[UUID],
    )

**Fields:**

- ``nodes`` — mapping from node UUID to ``BasicPlan``.
- ``edges`` — set of ``(from_node_uuid, to_node_uuid)`` pairs.  An edge from
  A to B means B may not start until A has completed.
- ``inputs`` — set of UUIDs of externally-supplied datasets.  Typically empty
  for query plans.
- ``outputs`` — set of UUIDs of the datasets that constitute the final result.

A ``DagPlan`` with no edges represents a fully-parallel structure (every node
may run concurrently).  A chain of edges expresses a serial dependency.

**Example — two parallel fetch steps followed by a join:**

.. code-block:: python

    from uuid import uuid4
    from src.lunch.query_engines.dag_plan import DagPlan

    fetch_region_id = uuid4()
    fetch_product_id = uuid4()
    join_id = uuid4()

    plan = DagPlan(
        nodes={
            fetch_region_id: BasicPlan("FetchRegionData", inputs={}, outputs={fetch_region_id: fetch_region_id}),
            fetch_product_id: BasicPlan("FetchProductData", inputs={}, outputs={fetch_product_id: fetch_product_id}),
            join_id: BasicPlan("JoinAndAggregate", inputs={fetch_region_id: fetch_region_id, fetch_product_id: fetch_product_id}, outputs={join_id: join_id}),
        },
        edges={(fetch_region_id, join_id), (fetch_product_id, join_id)},
        inputs=set(),
        outputs={join_id},
    )


QueryResult
-----------

*Module:* ``src.lunch.query_engines.query_result``

The return value of the enactor after executing a ``DagPlan``.

.. code-block:: python

    QueryResult(
        data: Any,
        query: FullySpecifiedFactQuery,
        plan: DagPlan,
    )

**Fields:**

- ``data`` — the result dataset (e.g. a ``pandas.DataFrame`` or Arrow Table).
- ``query`` — the ``FullySpecifiedFactQuery`` that produced this result.
- ``plan`` — the ``DagPlan`` that was enacted (for debugging and profiling).


QuerySpecifier
--------------

*Module:* ``src.lunch.query_engines.query_specifier``

Abstract ``Conductor`` base for all query specifiers.  Defines the interface:

.. code-block:: python

    async def specify(self, query: Query) -> FullySpecifiedFactQuery:
        ...

Subclasses inject the appropriate ``VersionManager``, ``ModelManager``, and
``Transformer`` helpers via their constructors.


CubeQueryResolver
-----------------

*Module:* ``src.lunch.query_engines.cube_query_resolver``

``Transformer`` that converts a vague ``CubeQuery`` plus a resolved
``Version`` and ``StarSchema`` into a ``FullySpecifiedFactQuery``.  Exposes
a single static method:

.. code-block:: python

    CubeQueryResolver.resolve(
        query: CubeQuery,
        version: Version,
        star_schema: StarSchema,
    ) -> FullySpecifiedFactQuery

**Shorthand mapping:**

- ``projection='default'`` → all dimensions and all measure ids from the ``StarSchema``.
  Any other value is treated as a dict ``{"dimensions": [...], "measures": [...]}``
  and passed through unchanged.
- ``aggregation='default'`` → ``['sum']``; any other value is passed through as a list.
- ``filter=None`` → ``[]``; any other value is passed through as a list.


CubeQuerySpecifier
------------------

*Module:* ``src.lunch.query_engines.cube_query_specifier``

Concrete ``Conductor`` for cube queries.  Holds references to a
``VersionManager``, a ``ModelManager``, and a ``CubeQueryResolver``.

``specify()`` steps:

1. If ``query.version == 'latest'``, calls ``version_manager.read_version()``
   to obtain a concrete ``Version``; otherwise uses ``query.version`` directly.
2. Calls ``model_manager.get_star_schema_model_by_fact_name(name=..., version=...)``
   to fetch the ``StarSchema``.
3. Delegates to ``CubeQueryResolver.resolve()`` (pure, no I/O).

Errors from either manager propagate to the caller unchanged.


Planner
-------

*Module:* ``src.lunch.query_engines.planner``

Abstract ``Conductor`` base for all planners.  Defines the interface:

.. code-block:: python

    async def plan(self, query: FullySpecifiedFactQuery) -> DagPlan:
        ...

Subclasses inject the appropriate ``FactDataStore`` and ``Transformer`` helpers
via their constructors.


CubeQueryDagBuilder
-------------------

*Module:* ``src.lunch.query_engines.cube_query_dag_builder``

``Transformer`` that builds a ``DagPlan`` from a ``FullySpecifiedFactQuery``
and a ``PartitionManifest`` (``dict[int, int]``, partition_id → cube_data_version).
Exposes a single static method:

.. code-block:: python

    CubeQueryDagBuilder.build(
        query: FullySpecifiedFactQuery,
        partitions: PartitionManifest,
    ) -> DagPlan

**Node types generated:**

- ``FetchDimensionData`` — one per entry in ``query.dimensions``.
- ``FetchFactData`` — one per entry in ``partitions``.
- ``JoinDimensionsToFact`` — present only when ``query.dimensions`` is non-empty;
  its inputs are all fetch-node output UUIDs.
- ``Aggregate`` — one per entry in ``query.aggregations``; its input is the join
  output UUID (or the fact-fetch output UUID when there are no dimensions).

All UUIDs are generated with ``uuid4()``.  Edges are wired so that each fetch
node precedes the join, and the join (or fact fetch) precedes each aggregate.

**Zero-dimension shortcut:**  When ``query.dimensions`` is empty, no
``FetchDimensionData`` or ``JoinDimensionsToFact`` nodes are created; each
``Aggregate`` node reads directly from the ``FetchFactData`` output.


CubeQueryPlanner
----------------

*Module:* ``src.lunch.query_engines.cube_query_planner``

Concrete ``Planner`` for cube queries.  Holds references to a
``FactDataStore`` and a ``CubeQueryDagBuilder``.

``plan()`` steps:

1. Calls ``fact_data_store.get_partition_manifest(version=query.version)``
   (async) to obtain the ``PartitionManifest``.
2. Delegates to ``CubeQueryDagBuilder.build()`` (pure, no I/O).
3. Returns the resulting ``DagPlan``.

Errors from ``get_partition_manifest`` propagate to the caller unchanged.

``FactDataStore.get_partition_manifest``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Added to:* ``src.lunch.storage.fact_data_store``

New public async method on ``FactDataStore``:

.. code-block:: python

    async def get_partition_manifest(self, version: Version) -> dict[int, int]:
        ...

Returns the partition index (partition_id → cube_data_version) at the
specified version.  Returns an empty dict if no partitions have been written.


QueryEnactor
------------

*Module:* ``src.lunch.query_engines.query_enactor``

Abstract ``Conductor`` base for all query enactors.  Defines the ``enact`` interface:

.. code-block:: python

    async def enact(self, plan: DagPlan) -> QueryResult:
        ...

Subclasses inject the appropriate data stores and ``Transformer`` helpers via
their constructors.


CubeQueryEnactor
----------------

*Module:* ``src.lunch.query_engines.cube_query_enactor``

Concrete ``QueryEnactor`` for cube queries.  Holds references to a
``FactDataStore`` and a ``DimensionDataStore``.  Builds a dispatch table in
``__init__`` and implements the DAG execution loop in ``enact``.

.. code-block:: python

    CubeQueryEnactor(
        fact_data_store: FactDataStore,
        dimension_data_store: DimensionDataStore,
    )

**DAG execution loop:**

1. Initialise an empty ``result_registry`` (``dict[UUID, Any]``).
2. Find all nodes whose UUID inputs are all present in ``result_registry``
   (initially: nodes with no UUID inputs).
3. Execute those nodes concurrently via ``asyncio.gather``.
4. Add each node's outputs to ``result_registry``.
5. Repeat until all nodes are done.
6. Collect data under ``plan.outputs`` UUIDs and wrap in a ``QueryResult``.

**Dispatch table (step name → module-level async handler):**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Step name
     - Handler responsibility
   * - ``FetchDimensionData``
     - Read dimension columns from ``DimensionDataStore`` using the
       ``dimension_id`` from the node's ``dimension`` input.
   * - ``FetchFactData``
     - Read fact partition columns from ``FactDataStore`` using
       ``partition_id`` and ``version`` from the node's inputs.
   * - ``JoinDimensionsToFact``
     - Perform an in-memory join of dimension data onto the fact table,
       keyed by dimension member ids.
   * - ``Aggregate``
     - Apply the specified aggregation function to the joined dataset.

New step types can be added by registering additional entries in the dispatch
table without changing the control-flow loop.


Source locations
----------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Class
     - Source path
   * - ``DagPlan``
     - ``src/lunch/query_engines/dag_plan.py``
   * - ``QueryResult``
     - ``src/lunch/query_engines/query_result.py``
   * - ``QuerySpecifier``
     - ``src/lunch/query_engines/query_specifier.py``
   * - ``CubeQueryResolver``
     - ``src/lunch/query_engines/cube_query_resolver.py``
   * - ``CubeQuerySpecifier``
     - ``src/lunch/query_engines/cube_query_specifier.py``
   * - ``Planner``
     - ``src/lunch/query_engines/planner.py``
   * - ``CubeQueryDagBuilder``
     - ``src/lunch/query_engines/cube_query_dag_builder.py``
   * - ``CubeQueryPlanner``
     - ``src/lunch/query_engines/cube_query_planner.py``
   * - ``QueryEnactor``
     - ``src/lunch/query_engines/query_enactor.py``
   * - ``CubeQueryEnactor``
     - ``src/lunch/query_engines/cube_query_enactor.py``

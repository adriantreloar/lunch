Query Engines
=============

This page documents the data classes used by the query engine pipeline.
These are pure ``Data`` subclasses — they carry no behaviour.

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

Plans
=====

A **Plan** is a pure-data description of work to be done.  Plans are created
by planners (``Transformer`` classes), enriched by optimisers (``Conductor``
classes), and executed by enactors (also ``Conductor`` classes).  Because they
carry no behaviour, plans can be inspected, logged, or modified before any
storage operation takes place.

All plan classes extend the ``Plan`` base class, which itself extends ``Data``
— the marker base class for pure data containers.

.. contents:: On this page
   :local:
   :depth: 2


Plan types
----------

BasicPlan
~~~~~~~~~

*Module:* ``src.lunch.plans.basic_plan``

The fundamental unit of work — a named local function call with typed inputs
and outputs.

.. code-block:: python

    BasicPlan(
        name: str,
        inputs: dict[str, Any],
        outputs: dict[str, Any],
    )

``name`` identifies the private function the enactor will invoke.  ``inputs``
carries the parameter values.  ``outputs`` maps logical output names to their
actual values (e.g. a ``Fact`` object); the UUID-as-handle reference mechanism
described below is the intended future design but is not yet implemented.

.. warning:: Not yet implemented

   The UUID-reference pattern for ``outputs`` is not yet implemented.
   Currently, ``outputs`` holds actual values directly (see the fact-append
   example below).  UUID resolution between steps is reserved for future
   distributed-execution support.

**Example — dimension import:**

.. code-block:: python

    BasicPlan(
        name="_import_locally_from_dataframe",
        inputs={
            "read_dimension":  None,          # None → first import
            "write_dimension": {"name": "Department", "id_": 2, ...},
            "data_columns":    {"department_id": dtype("O"), "name": dtype("O")},
            "merge_key":       [0],
            "read_filter":     {},
            "read_dimension_storage_instructions":  {...},
            "write_dimension_storage_instructions": {...},
        },
        outputs={},
    )

**Example — fact append:**

.. code-block:: python

    BasicPlan(
        name="_import_fact_append_locally_from_dataframe",
        inputs={
            "source_definition": {
                "type": "pd.DataFrame",
                "length": 1000,
                "columns": {"department_id": "object", "sales": "float64"},
            },
            "read_fact":        <Fact at read_version>,
            "column_id_mapping": {"department_id": 0, "sales": 3},
            "merge_key":        [0],
            "read_filter":      None,
        },
        outputs={
            "write_fact": <Fact at write_version>,
        },
    )

The ``repr`` of a ``BasicPlan`` shows the function name and the keys of
``inputs`` / ``outputs`` but **not** their values, keeping log output concise:

.. code-block:: python

    >>> repr(plan)
    "BasicPlan('_import_locally_from_dataframe', inputs=['read_dimension', 'merge_key'], outputs=[])"

DagPlan
~~~~~~~

*Module:* ``src.lunch.plans.dag_plan``

A directed acyclic graph of ``BasicPlan`` nodes.  ``DagPlan`` generalises
both serial and parallel execution: independent nodes are executed
concurrently (``asyncio.gather``); nodes whose inputs reference UUID outputs
of earlier nodes are executed after those nodes complete.

.. code-block:: python

    DagPlan(
        nodes:   dict[UUID, BasicPlan],
        edges:   Set[Tuple[UUID, UUID]],
        inputs:  Set[UUID],
        outputs: Set[UUID],
    )

``nodes`` maps a node UUID to its ``BasicPlan``.  ``edges`` records
``(dependent, dependency)`` pairs, but enactors derive execution order from
UUID value references in ``node.inputs`` rather than from ``edges`` directly.
``inputs`` and ``outputs`` name the externally-supplied and final-result
dataset UUIDs respectively.

**Example — two sequential dimension imports where the second reads the
output of the first:**

.. code-block:: python

    from uuid import uuid1

    node_a, node_b, handle = uuid1(), uuid1(), uuid1()

    DagPlan(
        nodes={
            node_a: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": None, "write_dimension": dim_a,
                        "read_filter": {}, "merge_key": [0]},
                outputs={"write_dimension": handle},
            ),
            node_b: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": handle, "write_dimension": dim_b,
                        "read_filter": {}, "merge_key": [0]},
                outputs={},
            ),
        },
        edges={(node_b, node_a)},
        inputs=set(),
        outputs=set(),
    )

The ``repr`` shows all nodes and edges:

.. code-block:: python

    >>> repr(plan)
    "DagPlan(nodes={...}, edges={...})"

RemotePlan
~~~~~~~~~~

*Module:* ``src.lunch.plans.remote_plan``

A remote procedure call — identical in structure to ``BasicPlan`` but with an
additional ``location`` field that specifies the RPC endpoint.

.. code-block:: python

    RemotePlan(
        location: str,
        name: str,
        inputs: dict[str, Any],
        outputs: dict[str, Any],
    )

**Example:**

.. code-block:: python

    RemotePlan(
        location="http://worker-1:5000",
        name="_import_locally_from_dataframe",
        inputs={
            "write_dimension": {"name": "Time", "id_": 3, ...},
            "merge_key":       [0],
            ...
        },
        outputs={},
    )

The ``repr`` includes the location:

.. code-block:: python

    >>> repr(plan)
    "RemotePlan('http://worker-1:5000', '_import_locally_from_dataframe', inputs=[...], outputs=[...])"

.. warning:: Not yet implemented

   ``RemotePlan`` is defined and tested but remote dispatch is not yet
   implemented in any enactor.  Any enactor receiving a ``RemotePlan`` will
   raise ``ValueError``.


Output references
-----------------

``DagPlan`` nodes communicate through UUID references: a node places a UUID
in its ``outputs`` dict and a later node places the same UUID as a value in
its ``inputs`` dict.  The enactor resolves these references automatically —
a node is only scheduled once all UUID values in its ``inputs`` are present
in the result registry.

.. code-block:: python

    from uuid import uuid1

    y_id = uuid1()
    node_a, node_b = uuid1(), uuid1()

    DagPlan(
        nodes={
            node_a: BasicPlan(name="produce_y", inputs={"x": 42}, outputs={"y": y_id}),
            node_b: BasicPlan(name="consume_y", inputs={"y": y_id}, outputs={}),
        },
        edges={(node_b, node_a)},
        inputs=set(),
        outputs=set(),
    )


How enactors dispatch on plans
-------------------------------

An enactor receives a plan and dispatches on its type and ``name``.  The
pattern used throughout the codebase is:

.. code-block:: python

    async def _enact_plan(import_plan, data, ...):
        if isinstance(import_plan, BasicPlan):
            if import_plan.name == "_import_locally_from_dataframe":
                await _import_locally_from_dataframe(
                    **import_plan.inputs, data=data, ...
                )
                return
        raise ValueError(import_plan)

Any plan type or name that the enactor does not recognise raises ``ValueError``.

**Dimension import dispatch** (``DimensionImportEnactor``):

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - Plan
     - Action
   * - ``DagPlan(...)``
     - Execute all nodes in dependency order, resolving UUID references between nodes.
   * - ``BasicPlan("_import_locally_from_dataframe", ...)``
     - Merge source DataFrame with existing dimension data and write to ``DimensionDataStore``.
   * - Anything else
     - ``ValueError``

**Fact import dispatch** (``FactImportEnactor``):

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - Plan
     - Action
   * - ``BasicPlan("_import_fact_append_locally_from_dataframe", ...)``
     - Rename columns, merge with existing fact data, and write to ``FactDataStore``.
   * - Anything else
     - ``ValueError``


Planner / optimiser / enactor roles
------------------------------------

.. code-block:: text

    Planner   (Transformer) ──► creates BasicPlan / RemotePlan / DagPlan
    Optimiser (Conductor)   ──► queries storage for hints, then calls the planner
    Enactor   (Conductor)   ──► receives plan + data, dispatches on type/name, writes storage

Keeping these three concerns separate means:

- Plans can be inspected or logged before any I/O occurs.
- The optimiser can substitute a ``RemotePlan`` for a ``BasicPlan``, or
  compose multiple ``BasicPlan`` nodes into a ``DagPlan``, without changing
  the enactor or the caller.
- Enactors remain thin delegation layers with no planning logic.
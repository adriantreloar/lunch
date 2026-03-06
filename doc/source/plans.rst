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
        outputs: dict[str, uuid1],
    )

``name`` identifies the private function the enactor will invoke.  ``inputs``
carries the parameter values.  ``outputs`` maps logical output names to UUIDs;
those UUIDs can be referenced as input values in later plan steps (see
`Output references`_ below).

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

SerialPlan
~~~~~~~~~~

*Module:* ``src.lunch.plans.serial_plan``

An ordered sequence of plans that must execute one after another.  Use
``SerialPlan`` when a later step depends on the result of an earlier one.

.. code-block:: python

    SerialPlan(
        steps: list[Plan],
    )

Each element of ``steps`` can itself be a ``BasicPlan``, ``RemotePlan``,
``ParallelPlan``, or another ``SerialPlan``, allowing arbitrary nesting.

**Example — two sequential local operations:**

.. code-block:: python

    SerialPlan(steps=[
        BasicPlan(name="step_one", inputs={"x": 1}, outputs={"y": uuid1()}),
        BasicPlan(name="step_two", inputs={"y": <uuid from step_one>}, outputs={}),
    ])

The ``repr`` includes each nested step's repr:

.. code-block:: python

    >>> repr(plan)
    "SerialPlan([BasicPlan('step_one', inputs=['x'], outputs=['y']), ...])"

ParallelPlan
~~~~~~~~~~~~

*Module:* ``src.lunch.plans.parallel_plan``

An unordered collection of independent plans that may execute concurrently.
Use ``ParallelPlan`` when the steps share no data dependencies.

.. code-block:: python

    ParallelPlan(
        steps: list[Plan],
    )

**Example — three independent dimension imports:**

.. code-block:: python

    ParallelPlan(steps=[
        BasicPlan(name="_import_locally_from_dataframe", inputs={"write_dimension": dim_a, ...}, outputs={}),
        BasicPlan(name="_import_locally_from_dataframe", inputs={"write_dimension": dim_b, ...}, outputs={}),
        BasicPlan(name="_import_locally_from_dataframe", inputs={"write_dimension": dim_c, ...}, outputs={}),
    ])

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
        outputs: dict[str, uuid1],
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

.. note::

   ``RemotePlan`` is defined and tested but remote dispatch is not yet
   implemented in any enactor.


Output references
-----------------

``BasicPlan`` and ``RemotePlan`` outputs are keyed by ``uuid1`` values.  The
same UUID can appear as an ``inputs`` value in a later step of a
``SerialPlan``, creating an explicit data-flow dependency.  The enactor is
responsible for resolving UUID references to the actual values produced by
earlier steps.

.. code-block:: python

    from uuid import uuid1

    y_id = uuid1()

    SerialPlan(steps=[
        BasicPlan(name="produce_y", inputs={"x": 42}, outputs={"y": y_id}),
        BasicPlan(name="consume_y", inputs={"y": y_id}, outputs={}),
    ])

.. note::

   UUID resolution between steps is not yet implemented in the current
   enactors.  It is reserved for future distributed-execution support.


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

    Planner   (Transformer) ──► creates BasicPlan / RemotePlan / SerialPlan / ParallelPlan
    Optimiser (Conductor)   ──► queries storage for hints, then calls the planner
    Enactor   (Conductor)   ──► receives plan + data, dispatches on type/name, writes storage

Keeping these three concerns separate means:

- Plans can be inspected or logged before any I/O occurs.
- The optimiser can substitute a ``RemotePlan`` for a ``BasicPlan`` (or wrap
  multiple ``BasicPlan`` steps in a ``ParallelPlan``) without changing the
  enactor or the caller.
- Enactors remain thin delegation layers with no planning logic.

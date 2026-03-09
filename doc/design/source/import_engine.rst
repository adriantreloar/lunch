Import Engine
=============

The import engine follows a **plan / optimise / enact** pattern:

- **Planner** (``Transformer``) — creates a ``Plan`` describing the steps.
- **Optimiser** (``Conductor``) — queries storage for hints, then calls the planner.
- **Enactor** (``Conductor``) — receives the plan and data, dispatches on type/name, writes to storage.

.. contents:: On this page
   :local:
   :depth: 2


Fact import
-----------

Fact data is appended via ``FactImportEnactor``, which dispatches on
``BasicPlan`` and ``SerialPlan`` instances.  The private function
``_import_fact_append_locally_from_dataframe`` performs the merge and write
for a single step.

``write_fact`` placement
~~~~~~~~~~~~~~~~~~~~~~~~~

The intended design places ``write_fact`` (the ``Fact`` object describing
the write target) in ``outputs`` of a ``BasicPlan``, following the
output-reference UUID mechanism described in the Plans reference
documentation.  The dimension enactor uses a different convention, placing
its equivalent (``write_dimension``) in ``inputs`` with ``outputs={}``; no
decision has been made on which convention the fact enactor should adopt.

.. warning:: Known breakage — UUID output handle for ``write_fact``

   There is a **failing test** that exposes an incomplete implementation of
   UUID output-reference resolution in ``FactImportEnactor``.

   **Failing test**::

       src/lunch/tests/import_engine/test_fact_import_enactor.py
       ::test_serial_plan_uuid_output_resolved_as_input_to_later_step

   The test constructs a ``SerialPlan`` with two steps.  Step 1 names its
   output ``write_fact`` under a ``uuid1()`` handle instead of a concrete
   ``Fact`` object::

       step1 = BasicPlan(
           name="_import_fact_append_locally_from_dataframe",
           inputs={"read_fact": ..., "column_id_mapping": ..., ...},
           outputs={"write_fact": handle},   # <-- UUID, not a Fact
       )
       step2 = BasicPlan(
           name="_import_fact_append_locally_from_dataframe",
           inputs={"read_fact": handle, ...},  # resolved from step1 output
           outputs={"write_fact": _write_fact},
       )

   The intent is that ``handle`` is resolved to the ``Fact`` written by
   step 1, so step 2 can read from it.

   **Why it fails**

   The UUID resolution infrastructure *is* in place (added in commit
   ``bcea962``, issue ``lunch-k5s``):

   - ``_resolve_inputs`` (``fact_import_enactor.py``, line 36) replaces any
     UUID value in ``inputs`` with the corresponding entry from
     ``output_store`` before a step runs.
   - ``_collect_outputs`` (``fact_import_enactor.py``, line 40) populates
     ``output_store`` from the ``result`` dict returned by a completed step,
     for every ``outputs`` entry whose value is a UUID handle.

   However, the step function itself reads ``write_fact`` from
   ``append_plan.outputs`` (``fact_import_enactor.py``, line 86)::

       write_fact = append_plan.outputs["write_fact"]

   When ``outputs["write_fact"]`` is a UUID handle rather than a ``Fact``
   object, the subsequent call on line 108::

       await fact_data_store.put(fact_id=write_fact.fact_id, ...)

   raises ``AttributeError: 'UUID' object has no attribute 'fact_id'``.

   Step 1 also has no concrete ``Fact`` in its ``inputs`` or ``outputs``
   for the UUID case, so there is no obvious target to write to.

   **Open question**

   Two fixes have been proposed; neither has been implemented:

   1. Move ``write_fact`` into ``inputs`` (matching the dimension enactor
      pattern), so planners and callers supply it as a normal input that can
      also be resolved via UUID from a prior step's output.  This requires
      updating ``FactAppendPlanner``, the enactor function, and all tests
      that construct fact ``BasicPlan`` instances.  Tracked in issue
      ``lunch-8se``.

   2. Keep ``write_fact`` in ``outputs`` but detect the UUID case inside
      ``_import_fact_append_locally_from_dataframe`` and fall back to
      ``read_fact`` as the write target.  This is a narrower change but
      produces semantics (write back to the read fact) that may not be
      generally correct.

   Until one approach is chosen and implemented, any ``SerialPlan`` that
   uses a UUID handle for ``write_fact`` will raise ``AttributeError`` at
   runtime.


SerialPlan UUID output-reference resolution
-------------------------------------------

The ``_enact_plan`` function (``fact_import_enactor.py``, lines 46–76)
handles ``SerialPlan`` by iterating over its steps and maintaining an
``output_store`` dictionary:

.. code-block:: python

    output_store: dict = {}
    for step in append_plan.steps:
        resolved = BasicPlan(
            name=step.name,
            inputs=_resolve_inputs(step.inputs, output_store),
            outputs=step.outputs,
        )
        result = await _enact_plan(resolved, ...)
        _collect_outputs(step.outputs, result, output_store)

``_resolve_inputs`` (line 36) substitutes any UUID value in ``inputs`` with
the stored result from a prior step.  ``_collect_outputs`` (line 40) stores
any UUID-keyed ``outputs`` entry from the step's return value into
``output_store`` so later steps can reference it.

This mechanism works correctly when ``write_fact`` is a concrete ``Fact``
object.  It breaks when ``write_fact`` is itself a UUID handle, because the
step function cannot determine what to write to (see warning above).

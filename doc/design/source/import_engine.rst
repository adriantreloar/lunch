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

Fact data is appended via ``FactImportEnactor``.  The private function
``_import_fact_append_locally_from_dataframe`` performs the merge and write
for a single ``BasicPlan`` step.

.. note::

   Query Planner and Query Enactor logic (including ``SerialPlan``,
   ``ParallelPlan``, UUID output-reference resolution, and multi-step plan
   composition) is **not ready to be applied to import functionality** at
   this stage.  Only read queries are currently being refined.  The import
   enactor therefore handles only ``BasicPlan`` instances, and complex plan
   support will be designed and added later.

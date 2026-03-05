Examples
========

The ``src/lunch/examples/`` directory contains a set of standalone scripts
that demonstrate the end-to-end flow for setting up the star schema, writing
dimension member data, and appending fact data.  Each script can be run
directly (``uv run python -m src.lunch.examples.<name>``) or imported by
other example scripts.

.. contents:: On this page
   :local:
   :depth: 2


Dependency order
----------------

The examples build on each other in this order::

    setup_managers          ← shared foundation (imported, not run directly)
        └─ save_dimension   ← writes dimension schemas
               └─ save_fact ← writes the fact schema

    setup_managers
        └─ inject_dimension_data  ← raw dimension member data write (lower-level)

    setup_managers
        └─ insert_dimension_data  ← managed dimension member data write (recommended)
               └─ insert_fact_data ← fact data import pipeline


``setup_managers.py``
---------------------

**Purpose:** Constructs and exposes the canonical ``model_manager`` and
``version_manager`` instances used by all other examples.

This module wires the full storage stack from scratch:

.. code-block:: text

    Persistors (local files under example_output/)
        └─ Serializers (YAML)
               └─ Stores (ModelStore, VersionStore)
                      └─ Managers (ModelManager, VersionManager)

All caches use ``Null*`` implementations (caching disabled).  The storage
root is resolved relative to the package root::

    example_output/          ← version files
    example_output/model/    ← dimension and fact schema YAML files

This module is **not** run directly — it is imported by the other examples.


``save_dimension.py``
---------------------

**Purpose:** Writes dimension *schema* definitions into the star schema model.

Demonstrates two separate versioned model transactions:

1. Write the ``Test`` dimension (attributes: ``foo``, ``bar``, ``baz``).
2. Write the ``Department`` and ``Time`` dimensions in a single transaction.

Each transaction uses ``version_manager.write_model_version`` and advances
``model_version``.  No member data (rows) are written here — only the schema.

**Run order:** Can be run standalone; ``setup_managers`` is imported
automatically.


``save_fact.py``
----------------

**Purpose:** Writes the ``Sales`` fact schema into the star schema model.

Calls ``save_dimension()`` first to ensure the ``Department`` and ``Time``
dimensions exist, then defines the ``f_sales`` fact:

- **Dimensions:** ``Department`` (storage column id 0, auto-promoted to 3 by
  ``fill_default_column_ids`` since 0 is falsy) and ``Time`` (storage column
  id 2).
- **Measures:** ``sales`` (decimal, precision 2; ``measure_id=1``).
- **Storage:** index columns ``[1]``; data columns ``[2, 0]``.

The fact references dimensions by name (``dimension_id=0``); the manager
resolves the canonical ``dimension_id`` from storage at the write version
before persisting.

**Run order:** Depends on ``save_dimension``.


``inject_dimension_data.py``
-----------------------------

**Purpose:** Writes dimension member data (reference data) using the
lower-level storage API directly, without going through
``ReferenceDataManager``.

This script is an earlier, exploratory example.  It wires up a
``ReferenceDataStore`` manually and calls ``store_dimension_stats`` and
``store_dimension_attribute`` directly to write columnar attribute data for
dimension 1.

Two versioned transactions are used:

1. A ``write_model_version`` transaction to define the ``Department`` and
   ``Time`` dimensions.
2. A ``write_reference_data_version`` transaction to store the actual
   attribute columns (member data) for dimension 1.

.. note::

   This script bypasses the import pipeline (planner / optimiser / enactor).
   Use ``insert_dimension_data.py`` for the recommended managed approach.

**Run order:** Standalone.


``insert_dimension_data.py``
-----------------------------

**Purpose:** Writes dimension member data for three dimensions using the
full managed import pipeline.  This is the **recommended** approach for
loading dimension data.

The script:

1. Calls ``save_dimension()`` to ensure the dimension schemas exist.
2. Constructs the full reference data storage stack:

   .. code-block:: text

       LocalFileColumnarDimensionDataPersistor
           └─ ColumnarDimensionDataSerializer
                  └─ DimensionDataStore

       LocalFileReferenceDataPersistor
           └─ YamlReferenceDataSerializer
                  └─ ReferenceDataStore
                         ├─ DimensionDataStore (above)
                         └─ HierarchyDataStore (stub)

3. Wires the import pipeline:
   ``DimensionImportPlanner`` → ``DimensionImportOptimiser`` →
   ``DimensionImportEnactor``, assembled into a ``ReferenceDataManager``.

4. Opens a single ``write_reference_data_version`` transaction and calls
   ``reference_data_manager.update_dimension_from_dataframe`` for each of
   the three dimensions:

   - **Test** — 3 rows (``foo``, ``bar``, ``baz`` with integer attributes).
   - **Department** — 1 000 rows (``A Thing 0`` … ``A Thing 999``).
   - **Time** — 72 rows (monthly periods from ``2020-01`` to ``2025-12``).

Debug-level logging is enabled when run directly, so the full list of files
written to disk is printed to stdout.

**Run order:** Depends on ``save_dimension``.  Can be run standalone.


``insert_fact_data.py``
-----------------------

**Purpose:** Demonstrates the full end-to-end fact data import pipeline,
from a raw ``pd.DataFrame`` to persisted columnar column files on disk.

The script:

1. Calls ``insert_dimension_data()`` and ``save_fact()`` to ensure the full
   model and reference data are present.
2. Builds a three-row ``Sales`` DataFrame with columns ``department_id``,
   ``thing 2``, and ``sales value``.
3. Declares a ``column_mapping`` that maps each source column to a target in
   the star schema (dimension foreign key or measure).
4. Constructs the fact data storage stack:

   .. code-block:: text

       LocalFileColumnarFactDataPersistor  (example_output/fact/)
           └─ ColumnarFactDataSerializer
                  └─ FactDataStore

5. Wires the fact import pipeline:
   ``FactAppendPlanner`` → ``FactImportOptimiser`` → ``FactImportEnactor``,
   assembled into a ``CubeDataManager``.
6. Opens a ``write_reference_data_version`` transaction, calls
   ``fact_import_optimiser.create_dataframe_append_plan`` to produce a plan,
   then passes the plan and the source DataFrame to
   ``cube_data_manager.append_fact_from_dataframe``.

Output files written under ``example_output/fact/<version>/``:

- ``fact_data.version.index.yaml`` — maps fact id → reference data version.
- ``fact_data/<fact_id>/column.<column_id>.column`` — one text file per
  storage column containing the column's values (one per line).

Debug-level logging is enabled when run directly, printing every file write.

**Run order:** Depends on ``insert_dimension_data`` and ``save_fact``.


Running the full sequence
-------------------------

To exercise the complete pipeline from scratch:

.. code-block:: bash

    # 1. Write dimension and fact schemas
    uv run python -m src.lunch.examples.save_fact

    # 2. Load dimension member data
    uv run python -m src.lunch.examples.insert_dimension_data

    # 3. Append fact data rows
    uv run python -m src.lunch.examples.insert_fact_data

All output is written under ``example_output/`` at the repository root.
Running ``insert_fact_data`` is sufficient to trigger the full chain — it
calls ``insert_dimension_data`` and ``save_fact`` internally.

Fact Import Pipeline
====================

The fact import pipeline loads rows from a ``pd.DataFrame`` into the MVCC
columnar fact store.  It follows the same **plan / optimise / enact** pattern
used by the dimension import pipeline, but operates on fact (measure) data
rather than dimension member data.

.. contents:: On this page
   :local:
   :depth: 2


Overview
--------

A caller provides:

- A source ``pd.DataFrame`` whose columns contain dimension foreign-key values
  and measure values.
- A ``column_mapping`` list that declares how each source column maps to a
  target in the star schema.
- A ``read_version`` (the version to merge against) and a ``write_version``
  (the version to persist at).

The pipeline resolves that mapping to storage column IDs, merges with any
existing fact data at the read version, then writes the result as a set of
columnar text files — one file per storage column.

The public entry point is ``CubeDataManager.append_fact_from_dataframe``.


Column mapping
--------------

``column_mapping`` is a list of dicts, one per source column.  Two target
forms are supported:

.. code-block:: python

    column_mapping = [
        # Dimension foreign-key column — maps to a FactDimensionMetadatum
        {"source": ["department_id"], "target": ["Department", "id_"]},

        # Another dimension column
        {"source": ["thing 2"], "target": ["Time", "thing 2"]},

        # Measure column — maps to a FactMeasureMetadatum
        {"source": ["sales value"], "measure target": ["measures", "sales"]},
    ]

For ``"target"`` entries the planner looks up the ``FactDimensionMetadatum``
in the write-version fact whose ``dimension_name`` matches the first element,
and takes that metadatum's ``column_id`` as the storage column id.

For ``"measure target"`` entries the planner looks up the
``FactMeasureMetadatum`` whose ``name`` matches the second element, and takes
its ``measure_id`` as the storage column id.

The resulting ``column_id_mapping`` (source column name → storage column id)
is embedded in the plan and used by the enactor to rename the DataFrame before
writing.


Components
----------

FactAppendPlanner
~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.fact_append_planner``

*Base class:* ``Transformer`` (stateless; only static methods).

``create_local_dataframe_append_plan`` receives the read- and write-version
star schemas, the source ``TableMetadata``, and the ``column_mapping``.  It
returns a ``BasicPlan`` with:

.. code-block:: python

    BasicPlan(
        name="_import_fact_append_locally_from_dataframe",
        inputs={
            "source_definition": {
                "type": "pd.DataFrame",
                "length": <row count>,
                "columns": {col_name: dtype_str, ...},
            },
            "read_fact":        <Fact from read_version_target_model>,
            "column_id_mapping": {source_col: storage_col_id, ...},
            "merge_key":        [<index column ids>],
            "read_filter":      None,
        },
        outputs={
            "write_fact": <Fact from write_version_target_model>,
        },
    )

The plan carries no mutable state; it is a pure data description of the work
to be done.

FactImportOptimiser
~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.fact_import_optimiser``

*Base class:* ``Conductor``.

A thin delegation layer that currently delegates directly to
``FactAppendPlanner.create_local_dataframe_append_plan``.  The optimiser is
the right place to add future logic such as choosing between local and remote
execution plans based on data size or available processors.

FactImportEnactor
~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.fact_import_enactor``

*Base class:* ``Conductor``.

``enact_plan`` dispatches on the plan name.  For
``_import_fact_append_locally_from_dataframe`` it calls the private function
``_import_fact_append_locally_from_dataframe``, which:

1. Extracts ``read_fact``, ``write_fact``, ``column_id_mapping``,
   ``merge_key``, and ``read_filter`` from the plan.
2. Renames the source DataFrame's columns using ``column_id_mapping``
   (``data.rename(columns=column_id_mapping)``), so column names become
   integer storage ids.
3. Attempts to read existing data from ``FactDataStore.get_columns`` at the
   read version.

   - On ``KeyError`` (no data exists yet for this fact) — uses the renamed
     DataFrame directly as the merged result.
   - Otherwise — uses ``FactDataFrameTransformer.merge`` to combine existing
     and incoming rows on the ``merge_key``.

4. Calls ``FactDataFrameTransformer.columnize`` to convert the merged
   DataFrame into a ``dict[int, list]`` (storage column id → values).
5. Calls ``FactDataStore.put`` to persist the columnar data.

CubeDataManager
~~~~~~~~~~~~~~~

*Module:* ``src.lunch.managers.cube_data_manager``

*Base class:* ``Conductor``.

The public API for fact data import.  Callers first produce a plan via the
optimiser, then hand both the plan and the source data to the manager:

.. code-block:: python

    plan = await fact_import_optimiser.create_dataframe_append_plan(
        read_version_target_model=read_schema,
        write_version_target_model=write_schema,
        source_metadata=source_metadata,
        column_mapping=column_mapping,
        read_version=read_version,
        write_version=write_version,
    )

    await cube_data_manager.append_fact_from_dataframe(
        plan=plan,
        source_data=df_data,
        read_version=read_version,
        write_version=write_version,
    )

The manager does not re-invoke the optimiser.  Planning and enactment are
deliberately separated so that callers can inspect or log the plan before
committing to the write.


Storage layer
-------------

The three-layer stack for fact data mirrors the dimension data stack:

.. code-block:: text

    FactDataStore  (Conductor)
        ├─ ColumnarFactDataSerializer  (Transformer)
        │      └─ LocalFileColumnarFactDataPersistor  (Stateful)
        └─ NullFactDataCache  (Stateful, caching disabled)

FactDataStore
~~~~~~~~~~~~~

*Module:* ``src.lunch.storage.fact_data_store``

Provides ``put`` and ``get_columns``.

**``put(fact_id, columnar_data, read_version, write_version)``**

1. Reads the fact version index at ``write_version`` (or falls back to
   ``read_version`` if not yet created).  The version index maps
   ``fact_id → reference_data_version`` and records which sub-version holds
   each fact's data.
2. Updates the version index to point the given ``fact_id`` at the new
   ``write_version``.
3. Persists the updated version index.
4. Persists the columnar data via the serializer and updates the cache.
5. Similarly reads, updates, and persists the partition version index.

**``get_columns(read_version, fact_id, column_types, filter)``**

Looks up the correct sub-version from the version index, then retrieves the
column data from cache (or serializer on a cache miss).  Raises ``KeyError``
if no data exists for the given ``fact_id`` at ``read_version`` — the enactor
uses this to detect a first import.

ColumnarFactDataSerializer
~~~~~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.storage.serialization.columnar_fact_data_serializer``

Reads and writes:

- **Version index** — a YAML file at
  ``<version>/fact_data.version.index.yaml`` mapping fact ids to their
  current reference data sub-version.  Missing files are treated as an empty
  index (``{}``).
- **Column files** — one text file per storage column, written via
  ``LocalFileColumnarFactDataPersistor.open_attribute_file_write``.

``get_partition_index`` and ``put_partition_index`` read and write a global
partition index file at ``<version>/fact_data.partition.index.yaml``.  The
file is a YAML mapping of partition id (int) to ``cube_data_version`` (int).
A missing file is treated as an empty index (``{}``).

LocalFileColumnarFactDataPersistor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Module:*
``src.lunch.storage.persistence.local_file_columnar_fact_data_persistor``

Manages file paths and open handles under a configurable root directory.
The on-disk layout is::

    <root>/
        <cube_data_version>/
            fact_data.version.index.yaml      ← fact_id → cube_data_version
            fact_data.partition.index.yaml    ← partition_id → cube_data_version
            fact_data/
                <fact_id>/
                    column.<column_id>.column   ← one value per line


Merge semantics
---------------

The merge step (``FactDataFrameTransformer.merge``) uses
``combine_first`` keyed on ``merge_key`` (the fact's ``storage.index_columns``
cast to a list).  Incoming rows take precedence over existing rows that share
the same key; rows present only in the existing data are preserved.

On a first import (``KeyError`` from ``get_columns``) the merge step is
skipped entirely and the renamed source DataFrame is written directly.


Version isolation
-----------------

Fact data inherits the MVCC guarantees of the version system.  A ``put``
at write version *W* never touches data written at any earlier version.  The
version index makes it possible to read data at any past read version
independently of ongoing writes.

Fact data writes use ``write_cube_data_version`` and the version index is
keyed by ``cube_data_version``, keeping fact/measure data isolated from the
dimension-member ``reference_data_version``.
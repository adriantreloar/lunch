Dimension Import Pipeline
=========================

The dimension import pipeline loads rows from a ``pd.DataFrame`` into the MVCC
columnar dimension store.  It follows the **plan / optimise / enact** pattern:
a planner builds a pure-data ``Plan``, an optimiser enriches it with storage
context, and an enactor executes it against the storage layer.

.. contents:: On this page
   :local:
   :depth: 2


Overview
--------

A caller provides:

- A source ``pd.DataFrame`` whose columns correspond to dimension attributes.
- A dimension ``name`` that identifies the target ``Dimension`` in the star
  schema.
- A ``read_version`` (the version to merge against) and a ``write_version``
  (the version to persist at).

The pipeline resolves the dimension definition from the model, merges the
incoming data with any existing member data at the read version, then writes
the result as a set of columnar text files — one file per attribute.

The public entry point is ``ReferenceDataManager.update_dimension_from_dataframe``.


Components
----------

DimensionImportPlanner
~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.dimension_import_planner``

*Base class:* ``Transformer`` (stateless; only static methods).

``create_local_dataframe_import_plan`` receives the read- and write-version
dimension dicts, storage instructions, the data column name/dtype mapping, a
merge key, and a read filter.  It returns a ``BasicPlan`` with:

.. code-block:: python

    BasicPlan(
        name="_import_locally_from_dataframe",
        inputs={
            "read_dimension":  <dimension dict at read_version, or None>,
            "write_dimension": <dimension dict at write_version>,
            "read_dimension_storage_instructions":  {...},
            "write_dimension_storage_instructions": {...},
            "data_columns": {col_name: dtype, ...},
            "merge_key":    [0],   # first-column key, hard-coded by optimiser
            "read_filter":  {},
        },
        outputs={},
    )

The plan carries no mutable state; it is a pure data description of the work
to be done.

DimensionImportOptimiser
~~~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.dimension_import_optimiser``

*Base class:* ``Conductor``.

Holds references to a ``DimensionImportPlanner``, a ``DimensionDataStore``,
and a ``ModelManager``.

``create_dataframe_import_plan`` performs the following steps:

1. **Read-version dimension lookup** — attempts
   ``model_manager.get_dimension_by_name(name, read_version, add_default_storage=True)``.

   - ``KeyError`` is swallowed and ``read_dimension`` is set to ``None``.
     This is the expected first-import scenario.
   - All other exceptions propagate immediately.

2. **Write-version dimension lookup** — calls
   ``model_manager.get_dimension_by_name(name, write_version, add_default_storage=True)``.
   Any exception propagates; the dimension must exist in the write version.

3. **Data introspection** — extracts column names and dtypes from the source
   DataFrame, and reads storage instructions from the ``DimensionDataStore``
   for both versions.

4. **Plan delegation** — calls
   ``DimensionImportPlanner.create_local_dataframe_import_plan`` with all
   gathered context and returns the resulting ``BasicPlan``.

DimensionImportEnactor
~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.import_engine.dimension_import_enactor``

*Base class:* ``Conductor``.

``enact_plan`` dispatches on the plan name.  For
``_import_locally_from_dataframe`` it calls the private function
``_import_locally_from_dataframe``, which:

1. Extracts ``read_dimension``, ``write_dimension``, ``merge_key``, and
   ``read_filter`` from the plan inputs.
2. Determines the merged DataFrame:

   - If ``read_dimension`` is ``None`` (first import) — uses the source
     DataFrame directly.
   - Otherwise — reads existing columns from ``DimensionDataStore.get_columns``
     at the read version.

     - On ``KeyError`` (no data at that version) — uses the source DataFrame
       directly.
     - Otherwise — reconstructs a comparison DataFrame via
       ``DimensionDataFrameTransformer.make_dataframe`` and calls
       ``DimensionDataFrameTransformer.merge`` to combine existing and incoming
       rows on the ``merge_key``.

3. Calls ``DimensionDataFrameTransformer.columnize`` to convert the merged
   DataFrame into a ``dict[int, list]`` (attribute id → column values).
4. Calls ``DimensionDataStore.put`` to persist the columnar data at the write
   version.

ReferenceDataManager
~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.managers.reference_data_manager``

*Base class:* ``Conductor``.

The public API for dimension data import.  Callers invoke a single method:

.. code-block:: python

    await reference_data_manager.update_dimension_from_dataframe(
        name="Department",
        data=df,
        read_version=read_version,
        write_version=write_version,
    )

Internally this calls the optimiser to produce a plan, then hands the plan
and source data to the enactor.  Planning and enactment are deliberately
separated so that callers can inspect or log the plan before committing to
the write.

Multiple dimensions can be imported within a single ``write_reference_data_version``
context manager:

.. code-block:: python

    async with version_manager.read_version() as read_version:
        async with version_manager.write_reference_data_version(read_version) as write_version:
            await reference_data_manager.update_dimension_from_dataframe(
                name="Test", data=df_test, read_version=read_version, write_version=write_version,
            )
            await reference_data_manager.update_dimension_from_dataframe(
                name="Department", data=df_dept, read_version=read_version, write_version=write_version,
            )
            await reference_data_manager.update_dimension_from_dataframe(
                name="Time", data=df_time, read_version=read_version, write_version=write_version,
            )


Storage layer
-------------

The three-layer stack for dimension data:

.. code-block:: text

    DimensionDataStore  (Conductor)
        ├─ ColumnarDimensionDataSerializer  (Transformer)
        │      └─ LocalFileColumnarDimensionDataPersistor  (Stateful)
        └─ NullDimensionDataCache  (Stateful, caching disabled)

``DimensionDataStore`` is itself owned by ``ReferenceDataStore``, which also
holds a ``HierarchyDataStore`` (placeholder, not yet implemented) and manages
the cross-cutting reference data version index.

DimensionDataStore
~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.storage.dimension_data_store``

Provides ``put`` and ``get_columns``.

**``put(dimension_id, columnar_data, read_version, write_version)``**

1. Reads the reference data version index at ``write_version`` (falling back
   to ``read_version`` if not yet created).  The index maps
   ``dimension_id → reference_data_version``.
2. Updates the index to point ``dimension_id`` at the current
   ``reference_data_version``.
3. Persists the updated version index.
4. Persists the columnar data via the serializer and updates the cache.

**``get_columns(read_version, dimension_id, column_types, filter)``**

Looks up the correct sub-version from the version index, then retrieves
column data from cache (or serializer on a cache miss).  Raises ``KeyError``
if no data exists for the given ``dimension_id`` at ``read_version`` — the
enactor treats this as a first-import signal.

ColumnarDimensionDataSerializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Module:* ``src.lunch.storage.serialization.columnar_dimension_data_serializer``

Reads and writes:

- **Version index** — a YAML file mapping dimension ids to their current
  ``reference_data_version``.  Missing files are treated as an empty index
  (``{}``).
- **Column files** — one text file per attribute, written via
  ``LocalFileColumnarDimensionDataPersistor.open_attribute_file_write``.

LocalFileColumnarDimensionDataPersistor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Module:*
``src.lunch.storage.persistence.local_file_columnar_dimension_data_persistor``

Manages file paths and open handles under a configurable root directory.
The on-disk layout is::

    <root>/
        <reference_data_version>/
            reference_data.version.index.yaml
            dimension_data/
                <dimension_id>/
                    column.<attribute_id>.column   ← one value per line


Transformer: DimensionDataFrameTransformer
------------------------------------------

*Module:* ``src.lunch.import_engine.transformers.dimension_dataframe_transformer``

*Base class:* ``Transformer`` (stateless; only static methods).

**``make_dataframe(columns, dtypes)``** — reconstructs a ``pd.DataFrame``
from the columnar dict returned by ``get_columns``.

**``merge(source_df, compare_df, key)``** — combines new and existing rows
using ``set_index(key).combine_first().reset_index()``.  Incoming rows take
precedence; rows present only in existing data are preserved.

**``column_types_from_dimension(dimension)``** — extracts a
``{attribute_id: np.dtype}`` mapping from a dimension dict.

**``columnize(data)``** — converts a ``pd.DataFrame`` into a
``{column_name: list}`` dict for storage.


Merge semantics
---------------

The merge step uses ``combine_first`` keyed on ``merge_key`` (currently
hard-coded to ``[0]``, i.e. the first column of the DataFrame).  Incoming rows
take precedence over existing rows that share the same key; rows present only
in the existing data are preserved.

On a first import — either because ``read_dimension`` is ``None`` or because
``get_columns`` raises ``KeyError`` — the merge step is skipped entirely and
the source DataFrame is written directly.


Version isolation
-----------------

Dimension data inherits the MVCC guarantees of the version system.  A ``put``
at write version *W* never touches data written at any earlier version.  The
version index makes it possible to read data at any past read version
independently of ongoing writes.

Dimension data writes use ``write_reference_data_version`` and the version
index is keyed by ``reference_data_version``, keeping dimension-member data
isolated from the fact/measure ``cube_data_version``.

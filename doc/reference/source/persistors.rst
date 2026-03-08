Persistors
==========

Persistors are the lowest layer of the storage stack.  They are ``Conductor``
classes whose single responsibility is to **hand out open file handles** (or
in-memory equivalents) so that serializers can read and write data without
knowing where the data lives.

Every persistor exposes a pair of context managers for each logical "file":
``open_*_read`` and ``open_*_write``.  Callers enter the context manager,
read or write the yielded stream, and the persistor handles seeking and
(in the local-file case) directory creation.

.. contents:: On this page
   :local:
   :depth: 2


Overview
--------

There are four storage domains, each with its own persistor hierarchy:

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Domain
     - Local-file persistor
     - In-memory (test) persistor
   * - Model (dimensions + facts schema)
     - ``LocalFileModelPersistor``
     - ``StringIOModelPersistor``
   * - MVCC version record
     - ``LocalFileVersionPersistor``
     - ``StringIOVersionPersistor``
   * - Reference data index
     - ``LocalFileReferenceDataPersistor``
     - ``StringIOReferenceDataPersistor``
   * - Columnar dimension data
     - ``LocalFileColumnarDimensionDataPersistor``
     - ``StringIOColumnarDimensionDataPersistor``
   * - Columnar fact data
     - ``LocalFileColumnarFactDataPersistor``
     - ``StringIOColumnarFactDataPersistor``

Each StringIO persistor is a subclass of its local-file counterpart.  It
inherits the path-computation helpers (e.g. ``dimension_file()``) and
overrides only the ``open_*`` context managers to serve ``io.StringIO``
buffers instead of real file handles.  The dict ``_files_by_path`` maps the
canonical ``Path`` (as computed by the parent) to the in-memory buffer,
keeping path semantics identical between the two implementations.


Base classes
------------

.. code-block:: text

    Persistor  (Conductor)
    ├── ModelPersistor
    │   ├── LocalFileModelPersistor
    │   │   └── StringIOModelPersistor
    │   └── LocalFileVersionPersistor        (currently inherits ModelPersistor)
    │       └── StringIOVersionPersistor
    ├── ReferenceDataPersistor
    │   └── LocalFileReferenceDataPersistor
    │       └── StringIOReferenceDataPersistor
    ├── DimensionDataPersistor
    │   └── LocalFileColumnarDimensionDataPersistor
    │       └── StringIOColumnarDimensionDataPersistor
    └── FactDataPersistor
        └── LocalFileColumnarFactDataPersistor
            └── StringIOColumnarFactDataPersistor

``Persistor`` itself extends ``Conductor`` — persistors hold state (the dict
of open-file handles, or the filesystem), but they contain no transformation
logic.


LocalFileModelPersistor
-----------------------

**Module:** ``src.lunch.storage.persistence.local_file_model_persistor``

Backs the model store by writing YAML files under a configurable root
directory.  Files are organised by model version:

.. code-block:: text

    <directory>/
      <version>/
        dimension.version.index.yaml
        dimension.name.index.yaml
        dimension.<id>.yaml
        fact.version.index.yaml
        fact.name.index.yaml
        fact.<id>.yaml

**Context managers exposed:**

* ``open_dimension_version_index_file_read/write(version)``
* ``open_dimension_name_index_file_read/write(version)``
* ``open_dimension_file_read/write(id_, version)``
* ``open_fact_version_index_file_read/write(version)``
* ``open_fact_name_index_file_read/write(version)``
* ``open_fact_file_read/write(id_, version)``

Write methods create parent directories automatically (``mkdir -p``).


StringIOModelPersistor
-----------------------

**Module:** ``src.lunch.storage.persistence.stringio_model_persistor``

Drop-in replacement for tests.  Stores one ``io.StringIO`` per canonical
path in ``_files_by_path``.  Reading a path that was never written raises
``FileNotFoundError``, matching local-file behaviour.  Each ``seek(0)`` after
yield ensures the buffer is rewound for subsequent reads.

``_files_by_path`` is initialised after ``super().__init__()`` because the
model persistor constructor does not perform any file I/O.


LocalFileVersionPersistor
--------------------------

**Module:** ``src.lunch.storage.persistence.local_file_version_persistor``

Manages a single MVCC version file:

.. code-block:: text

    <directory>/
      _version.yaml

The file is created (or truncated to empty) during ``__init__`` so that
reads always have a target.  The ``VersionStore`` serializer is responsible
for the YAML content.

**Context managers exposed:**

* ``open_version_file_read()``
* ``open_version_file_write()``


StringIOVersionPersistor
-------------------------

**Module:** ``src.lunch.storage.persistence.stringio_version_persistor``

In-memory replacement.  Because the parent constructor calls
``open_version_file_write()`` to create the initial file, ``_files_by_path``
**must be initialised before** ``super().__init__()`` is called:

.. code-block:: python

    def __init__(self, directory: Path):
        self._files_by_path = {}          # must come first
        super().__init__(directory=directory)

After construction the version buffer exists and is empty, matching the
behaviour of a freshly created local file.


LocalFileReferenceDataPersistor
--------------------------------

**Module:** ``src.lunch.storage.persistence.local_file_reference_data_persistor``

Backs the reference-data version index:

.. code-block:: text

    <directory>/
      <version>/
        dimension_data/
          version.index.yaml

**Context managers exposed:**

* ``open_dimension_data_version_index_file_read/write(version)``


StringIOReferenceDataPersistor
-------------------------------

**Module:** ``src.lunch.storage.persistence.stringio_reference_data_persistor``

In-memory replacement.  Stores one ``io.StringIO`` per version in
``_files_by_path``.  Reading an unwritten version raises
``FileNotFoundError``.


LocalFileColumnarDimensionDataPersistor
----------------------------------------

**Module:** ``src.lunch.storage.persistence.local_file_columnar_dimension_data_persistor``

Backs columnar dimension data — one file per (dimension, attribute, version)
triple, plus a version-index file:

.. code-block:: text

    <directory>/
      <version>/
        dimension_data/
          <dimension_id>/
            attribute.<attribute_id>.column
        dimension_data.version.index.yaml

**Context managers exposed:**

* ``open_attribute_file_read/write(dimension_id, attribute_id, version)``
* ``open_version_index_file_read/write(version)``

**Path helpers** (also available on the StringIO subclass):

* ``attribute_file(dimension_id, attribute_id, version) → Path``
* ``dimension_version_index_file(version) → Path``


StringIOColumnarDimensionDataPersistor
---------------------------------------

**Module:** ``src.lunch.storage.persistence.stringio_columnar_dimension_data_persistor``

In-memory replacement.  Overrides all four ``open_*`` context managers.
The ``_files_by_path`` dict is keyed by the full ``Path`` returned by the
parent path helpers, so path semantics are preserved without touching the
filesystem.


LocalFileColumnarFactDataPersistor
-----------------------------------

**Module:** ``src.lunch.storage.persistence.local_file_columnar_fact_data_persistor``

Backs columnar fact data — one file per (fact_id/dimension_id, column,
version) triple, plus a version-index file:

.. code-block:: text

    <directory>/
      <version>/
        fact_data/
          <fact_id>/
            column.<attribute_id>.column
            partition.version.index.yaml
        fact_data.version.index.yaml

**Context managers exposed:**

* ``open_version_index_file_read/write(version)``
* ``open_attribute_file_read/write(dimension_id, attribute_id, version)``

**Path helpers:**

* ``fact_version_index_file(version) → Path``
* ``fact_partition_version_index_file(fact_id, cube_data_version) → Path``


StringIOColumnarFactDataPersistor
----------------------------------

**Module:** ``src.lunch.storage.persistence.stringio_columnar_fact_data_persistor``

In-memory replacement.  Adds a private helper
``_fact_attribute_file_path(dimension_id, attribute_id, version)`` that
mirrors the inline path computation in the parent, allowing the ``open_*``
overrides to key the ``_files_by_path`` dict consistently.


Testing persistors
------------------

Persistors are ``Stateful`` classes.  The key invariant to test is
**round-trip fidelity**: content written through ``open_*_write`` must be
readable via the corresponding ``open_*_read``.

Because StringIO persistors require no filesystem access, all tests are
synchronous and require no temporary directories.  Key scenarios to cover:

1. **Happy-path round-trip** — write content, read it back, assert equality.
2. **Key isolation** — different (id, version) tuples must not collide.
3. **Repeatable reads** — the same buffer must yield the same content on
   successive reads (the ``seek(0)`` contract).
4. **Missing key raises** ``FileNotFoundError`` — reading before writing must
   raise, matching local-file semantics.

See ``src/lunch/tests/storage/persistence/`` for the full test suite.
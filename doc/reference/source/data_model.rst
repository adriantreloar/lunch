Data Model
==========

lunch uses a **star schema** as its logical data model: a central *fact* table
surrounded by *dimension* tables.  All schema objects are immutable
(pyrsistent ``PClass`` for facts, plain dicts for dimensions) and are versioned
through the MVCC system.

.. contents:: On this page
   :local:
   :depth: 2


Star Schema
-----------

A :class:`StarSchema` is the top-level container returned by
``ModelManager.get_star_schema_model_by_fact_name``.  It bundles a single
``Fact`` together with all of its referenced dimensions.

.. code-block:: python

    class StarSchema(PClass):
        fact        # Fact  — the central fact object
        dimensions  # pmap  — dimension_id (int) → dimension dict

The ``dimensions`` map is keyed by integer ``dimension_id``, matching the
``dimension_id`` values stored in each ``FactDimensionMetadatum`` (see
`Fact Dimension Links`_ below).

``StarSchema`` is a read-only view assembled at query time.  It is **not**
persisted directly; facts and dimensions are stored independently and joined
by the manager layer.


Dimensions
----------

A dimension defines a set of member attributes that can be used to slice and
filter cube data — for example a *Department* dimension with an attribute
``name``, or a *Time* dimension with attributes ``period``, ``year``, and
``month``.

Dimensions are stored as plain Python dicts and validated by
``DimensionStructureValidator``.

Structure
~~~~~~~~~

.. code-block:: python

    {
        "name":          str,          # unique human-readable name, e.g. "Department"
        "id_":           int,          # assigned by storage on first write
        "model_version": int,          # the model version at which this was last written
        "attributes": [               # ordered list of attribute descriptors
            {
                "name": str,          # attribute name, e.g. "period"
                "id_":  int,          # assigned by DimensionTransformer if absent
            },
            ...
        ],
        "storage":       dict,        # optional — backend storage hints
    }

- ``name`` is mandatory and must be non-empty.
- ``id_`` and ``attributes`` are optional on input; they are filled in by
  ``DimensionTransformer`` before the dimension is written to storage.
- Attribute ``id_`` values are auto-incremented by
  ``DimensionTransformer.add_attribute_ids_to_dimension`` if not provided.

Reference Data
~~~~~~~~~~~~~~

The *reference data* layer (``ReferenceDataStore``) provides a unified,
versioned interface to two sub-stores:

- ``DimensionDataStore`` — holds the **member data** for each dimension: the
  actual rows that populate the attribute columns.  Each dimension's member
  data is stored as columnar arrays, one array per attribute, accessed by
  ``(dimension_id, attribute_id)``.
- ``HierarchyDataStore`` — holds parent–child relationship data for dimension
  hierarchies, keyed by ``(dimension_id, reference_data_version)``.

Both types of data are grouped under a **single** ``reference_data_version``
sub-version.  This is intentional: dimensions and hierarchies change at a much
lower frequency than fact (cube) data, so sharing one version counter keeps
the MVCC version record compact and avoids unnecessary version churn.

Hierarchies
~~~~~~~~~~~

A hierarchy defines a parent–child ordering of members within a dimension —
for example a *Time* dimension might expose a ``Year → Quarter → Month``
hierarchy.

``Hierarchy`` (*Module:* ``src.lunch.model.hierarchy``) is the schema-level
descriptor for a hierarchy:

.. code-block:: python

    Hierarchy(
        dimension_id: int,   # which dimension this hierarchy belongs to
        name: str,           # e.g. "YQM" for Year-Quarter-Month
    )

``HierarchyDataStore`` (*Module:* ``src.lunch.storage.hierarchy_data_store``)
stores the actual parent-child pair data for each dimension.  Pairs are
``[parent_member_id, child_member_id]`` integer lists.

.. code-block:: python

    await store.put(
        dimension_id: int,
        pairs: list,            # list of [parent_id, child_id] pairs
        read_version: Version,
        write_version: Version,
    ) -> None

    await store.get_pairs(
        dimension_id: int,
        read_version: Version,
    ) -> list

Both hierarchy data and dimension member data share the same
``reference_data_version`` sub-version.  The version index maps
``dimension_id → reference_data_version`` and is updated on every ``put``.

The storage stack follows the same three-layer pattern as ``DimensionDataStore``:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Layer
     - Class
   * - Store
     - ``src/lunch/storage/hierarchy_data_store.py``
   * - Serializer (abstract)
     - ``src/lunch/storage/serialization/hierarchy_data_serializer.py``
   * - Serializer (YAML)
     - ``src/lunch/storage/serialization/yaml_hierarchy_data_serializer.py``
   * - Serializer (null)
     - ``src/lunch/storage/serialization/null_hierarchy_data_serializer.py``
   * - Persistor (abstract)
     - ``src/lunch/storage/persistence/hierarchy_data_persistor.py``
   * - Persistor (StringIO)
     - ``src/lunch/storage/persistence/stringio_hierarchy_data_persistor.py``
   * - Cache (abstract)
     - ``src/lunch/storage/cache/hierarchy_data_cache.py``
   * - Cache (null)
     - ``src/lunch/storage/cache/null_hierarchy_data_cache.py``


Facts
-----

A ``Fact`` is the central table of a star schema.  It defines the measures
(numeric values) and the dimension references (foreign keys) that together
describe a single business event or metric.

``Fact`` is a pyrsistent ``PClass``, making all instances immutable.
Modifications return new instances.

Structure
~~~~~~~~~

.. code-block:: python

    class Fact(PClass):
        name          # str  — unique name, e.g. "Sales"
        fact_id       # int  — assigned by storage on first write (optional on input)
        model_version # int  — model version at which this was last written
        dimensions    # _FactDimensionsMetadata — ordered vector of FactDimensionMetadatum
        measures      # _FactMeasuresMetadata   — vector of FactMeasureMetadatum
        storage       # FactStorage             — describes which column ids are index vs data

Measures
~~~~~~~~

Each measure is described by a ``FactMeasureMetadatum``:

.. code-block:: python

    class FactMeasureMetadatum(PClass):
        name       # str  — e.g. "sales_value"
        measure_id # int  — unique within this fact
        type       # str  — data type string, e.g. "float64"
        precision  # int  — optional decimal precision

Measures are stored in the *data columns* of ``FactStorage`` (see
`Storage Layout`_ below).

Storage Layout
~~~~~~~~~~~~~~

``FactStorage`` describes how the fact's columns map to physical storage:

.. code-block:: python

    class FactStorage(PClass):
        index_columns  # _ColumnIds — column ids used as the composite sort/index key
        data_columns   # _ColumnIds — column ids holding measure values

- **index columns** — the dimension foreign-key columns that identify a row
  (e.g. ``department_id``, ``time_id``).  These are used for partitioning,
  sorting, and lookups.
- **data columns** — the measure value columns (e.g. ``sales_value``).

All column ids within a ``FactStorage`` must be non-negative integers and
unique within ``index_columns``.


Fact Dimension Links
--------------------

A fact references dimensions through its ``dimensions`` field, which is a
vector of ``FactDimensionMetadatum`` objects — one per dimension foreign key.

.. code-block:: python

    class FactDimensionMetadatum(PClass):
        name           # str  — column name in the fact (e.g. "department_id")
        view_order     # int  — display ordering hint (mandatory)
        column_id      # int  — column id in FactStorage.index_columns
        dimension_name # str  — matches Dimension["name"] (e.g. "Department")
        dimension_id   # int  — matches Dimension["id_"]

Either ``dimension_id`` or ``dimension_name`` (or both) must be set.  The
manager fills in the missing half from storage before writing.

Resolving a Reference
~~~~~~~~~~~~~~~~~~~~~

When a ``Fact`` is written via ``_update_model``, the following resolution
takes place for each ``FactDimensionMetadatum``:

1. If ``dimension_id`` is non-zero, the canonical dimension is loaded from
   storage at ``write_version`` by id.
2. Otherwise the canonical dimension is looked up by ``dimension_name``.
3. ``FactTransformer.fill_dimension_info`` then fills whichever of
   ``dimension_id`` / ``dimension_name`` was missing.

This means callers may supply either identifier; the stored ``Fact`` always
carries both.

Reference Integrity
~~~~~~~~~~~~~~~~~~~

Removing a ``FactDimensionMetadatum`` from a fact (i.e. dropping a dimension
reference) is a **breaking change** because existing cube data still contains
the corresponding foreign-key column.  This is enforced by
``FactReferenceValidator``:

- ``FactComparer.compare(previous_fact, new_fact)`` computes the set of
  ``dimension_id`` values that have been removed.
- ``FactReferenceValidator.validate(comparison)`` raises
  ``FactValidationError`` if that set is non-empty.

This check runs inside ``_check_and_put_fact`` before any write reaches
storage.

The symmetric check for **dimension** schema changes (removing an attribute
from a dimension) is enforced by ``DimensionReferenceValidator``:

- ``DimensionComparer.compare(previous_dimension, new_dimension)`` computes
  the set of attribute ``id_`` values that have been removed.
- ``DimensionReferenceValidator.validate(comparison)`` raises
  ``DimensionValidationError`` if that set is non-empty.

This check runs inside ``_check_and_put_dimension`` before any write reaches
storage.

Version Isolation
~~~~~~~~~~~~~~~~~

The ``Version`` object carries one sub-version per data domain:

- ``model_version`` — advances when the star schema definition changes (facts
  or dimension schemas).
- ``reference_data_version`` — advances when dimension member data **or**
  hierarchy data changes.  Both are grouped under a single counter because
  they change infrequently compared with fact data, making a shared version
  number more compact.
- ``cube_data_version`` — advances on every fact data import.

A ``StarSchema`` read at a given ``Version`` is therefore a consistent
snapshot: the fact schema, dimension schemas, member data, and hierarchies are
all pinned to the versions present at read time.
Query Specifier
===============

.. note::

   **Implemented.**  See ``src/lunch/query_engines/query_specifier.py``,
   ``src/lunch/query_engines/cube_query_resolver.py``, and
   ``src/lunch/query_engines/cube_query_specifier.py``.

A **QuerySpecifier** is a ``Conductor`` that transforms a vague, high-level
query into a ``FullySpecifiedFactQuery`` — a query in which every field is
concrete and unambiguous.  It does this by combining the caller's intent with
information retrieved from the Fact Schema (via the ``VersionManager`` and
``ModelManager``).

The ``QuerySpecifier`` sits at the start of the query pipeline, before the
:doc:`query_planner`.

.. contents:: On this page
   :local:
   :depth: 2


Responsibilities
----------------

A ``QuerySpecifier``:

* Resolves vague version references (e.g. ``"latest"``) into a concrete MVCC
  ``Version`` object by consulting the ``VersionManager``.
* Retrieves the ``StarSchema`` for the requested fact by consulting the
  ``ModelManager``.
* Determines the full list of dimensions, measures, filters, and aggregations
  implied by the caller's shorthand (e.g. ``"default"`` projection →
  enumerated dimension and measure ids).
* Returns a ``FullySpecifiedFactQuery`` that the :doc:`query_planner` can act
  on directly — no further schema lookups are required downstream.

The ``QuerySpecifier`` is a ``Conductor``: it holds references to the
``VersionManager`` and ``ModelManager`` and delegates all data transformation
to ``Transformer`` helpers.  It performs no transformations itself.


FullySpecifiedFactQuery
-----------------------

``FullySpecifiedFactQuery`` is a ``Data`` subclass (see :doc:`queries`).  It
carries everything the :doc:`query_planner` needs:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Description
   * - ``star_schema``
     - The resolved ``StarSchema`` object.
   * - ``version``
     - The concrete MVCC ``Version`` at which data should be read.
   * - ``dimensions``
     - The explicit list of ``Dimension`` objects to include.
   * - ``measures``
     - The explicit list of measure ids to retrieve.
   * - ``filters``
     - Zero or more filter predicates, each fully typed and bound to a
       dimension or measure id.
   * - ``aggregations``
     - The explicit aggregation functions to apply (e.g. ``["sum"]``).

Because ``FullySpecifiedFactQuery`` is a ``Data`` object it carries no
behaviour: it can be inspected, logged, or passed between components without
side effects.


Specification process
---------------------

.. code-block:: python

    fully_specified_query = await query_specifier.specify(query=vague_query)

Internally the ``CubeQuerySpecifier`` performs the following steps:

1. Call ``version_manager.get_current_version()`` (or resolve the caller's
   explicit version) to obtain a concrete ``Version``.
2. Call ``model_manager.get_star_schema(name=..., version=...)`` to fetch the
   ``StarSchema``.
3. Delegate to a ``Transformer`` (e.g. ``CubeQueryResolver``) to combine the
   vague query fields with the retrieved schema and produce the
   ``FullySpecifiedFactQuery``.

Steps 1 and 2 involve I/O and are ``await``-ed.  Step 3 is a pure
transformation with no I/O.


Classes
-------

QuerySpecifier
~~~~~~~~~~~~~~

**Role:** ``Conductor``

Abstract base for all query specifiers.  Defines the ``specify`` interface:

.. code-block:: python

    async def specify(self, query: Query) -> FullySpecifiedFactQuery:
        ...

Subclasses inject the appropriate ``VersionManager``, ``ModelManager``, and
``Transformer`` helpers via their constructors.

**Suggested source location:**
``src/lunch/query_engines/query_specifier.py``

CubeQuerySpecifier
~~~~~~~~~~~~~~~~~~

**Role:** ``Conductor``

Concrete specifier for cube queries.  Holds references to a
``VersionManager``, a ``ModelManager``, and a ``CubeQueryResolver``
transformer.

**Suggested source location:**
``src/lunch/query_engines/cube_query_specifier.py``

CubeQueryResolver
~~~~~~~~~~~~~~~~~

**Role:** ``Transformer``

Stateless transformer that combines a vague ``CubeQuery``, a resolved
``Version``, and a ``StarSchema`` into a ``FullySpecifiedFactQuery``.  Exposes
a single static method:

.. code-block:: python

    CubeQueryResolver.resolve(
        query: CubeQuery,
        version: Version,
        star_schema: StarSchema,
    ) -> FullySpecifiedFactQuery

**Suggested source location:**
``src/lunch/query_engines/cube_query_resolver.py``

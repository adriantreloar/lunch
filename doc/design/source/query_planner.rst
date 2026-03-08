Query Planner
=============

.. note::

   **Implemented.**  ``Planner``, ``CubeQueryPlanner``, and
   ``CubeQueryDagBuilder`` are implemented.  See the reference documentation
   at ``doc/reference/source/query_engines.rst`` for source locations and
   detailed API notes.

A **Planner** is a ``Conductor`` that converts a ``FullySpecifiedFactQuery``
(produced by the :doc:`query_specifier`) into a DAG Plan that describes every
retrieval, filter, and combination step needed to answer the query.

The ``Planner`` sits in the middle of the query pipeline, between the
:doc:`query_specifier` and the :doc:`query_enactor`.

.. contents:: On this page
   :local:
   :depth: 2


Responsibilities
----------------

A ``Planner``:

* Inspects the ``FullySpecifiedFactQuery`` and determines which storage
  partitions, dimension columns, and fact columns must be read.
* Decides which steps can be performed concurrently (no data dependency between
  them) and which must be performed sequentially (one step consumes the output
  of another).
* Assigns a UUID to every intermediate dataset produced by a step, so that the
  :doc:`query_enactor` can wire step inputs to step outputs unambiguously.
* Returns a DAG Plan — a directed acyclic graph of ``BasicPlan`` nodes — that
  the :doc:`query_enactor` can execute.

The ``Planner`` is a ``Conductor``: it may consult storage to discover
partition metadata, and it delegates plan-construction decisions to
``Transformer`` helpers.  It performs no data retrieval itself.


DAG Plan structure
------------------

See :doc:`query_engines` for the full description of the DAG Plan format.  In
brief:

* Each **node** is a ``BasicPlan`` — a named step with a set of input UUIDs
  and a set of output UUIDs.
* Each **edge** from node A to node B means "B requires the outputs of A".
* Nodes with no unsatisfied inputs may be started immediately and can run
  concurrently.
* UUIDs are the mechanism by which data flows along edges: the output UUID
  produced by one node is used as an input UUID by any dependent node.

The ``Planner`` is responsible for generating all UUIDs and constructing the
full edge set.


Planning process
----------------

.. code-block:: python

    plan = await planner.plan(fully_specified_query)

Internally a ``CubeQueryPlanner`` performs the following steps:

1. Inspect ``fully_specified_query.dimensions`` and assign one
   ``FetchDimensionData`` node per dimension (these are independent and carry
   no edges between them).
2. Assign a ``FetchFactData`` node for each required fact partition (also
   independent).
3. Assign a ``JoinDimensionsToFact`` node whose inputs are the output UUIDs of
   the fetch nodes from steps 1 and 2.
4. Assign one aggregation node per requested aggregation function, taking the
   join output UUID as its input.
5. Wire all edges and return the completed DAG Plan.

Steps that involve I/O (e.g. consulting a partition index to enumerate
partitions) are ``await``-ed.  Steps that only compute the plan structure are
pure transformations with no I/O.


Example DAG
-----------

For a query over two dimensions (Region, Product) and one fact table (Sales):

.. code-block:: text

    ┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
    │  FetchDimensionData  │    │  FetchDimensionData  │    │  FetchFactData       │
    │  dimension=Region    │    │  dimension=Product   │    │  fact=Sales          │
    │  out: uuid-region    │    │  out: uuid-product   │    │  out: uuid-fact      │
    └──────────┬───────────┘    └──────────┬───────────┘    └──────────┬───────────┘
               │                           │                           │
               └──────────────┬────────────┘                           │
                              │◄──────────────────────────────────────┘
                              ▼
               ┌──────────────────────────────┐
               │  JoinDimensionsToFact        │
               │  in:  uuid-region,           │
               │       uuid-product,          │
               │       uuid-fact              │
               │  out: uuid-joined            │
               └──────────────┬───────────────┘
                              │
                              ▼
               ┌──────────────────────────────┐
               │  Aggregate (sum)             │
               │  in:  uuid-joined            │
               │  out: uuid-result            │
               └──────────────────────────────┘


Classes
-------

Planner
~~~~~~~

**Role:** ``Conductor``

Abstract base for all planners.  Defines the ``plan`` interface:

.. code-block:: python

    async def plan(self, query: FullySpecifiedFactQuery) -> DagPlan:
        ...

Subclasses inject the appropriate storage accessors and ``Transformer``
helpers via their constructors.

**Suggested source location:**
``src/lunch/query_engines/planner.py``

CubeQueryPlanner
~~~~~~~~~~~~~~~~

**Role:** ``Conductor``

Concrete planner for cube queries.  Consults partition metadata from the
``FactDataStore`` and delegates DAG construction to ``CubeQueryDagBuilder``.

**Suggested source location:**
``src/lunch/query_engines/cube_query_planner.py``

CubeQueryDagBuilder
~~~~~~~~~~~~~~~~~~~

**Role:** ``Transformer``

Stateless transformer that, given a ``FullySpecifiedFactQuery`` and a partition
manifest (already fetched by the ``CubeQueryPlanner``), constructs and returns
the DAG Plan.  Generates all UUIDs and wires all edges.  Exposes a single
static method:

.. code-block:: python

    CubeQueryDagBuilder.build(
        query: FullySpecifiedFactQuery,
        partitions: PartitionManifest,
    ) -> DagPlan

**Suggested source location:**
``src/lunch/query_engines/cube_query_dag_builder.py``

DagPlan
~~~~~~~

**Role:** ``Data``

Pure data container representing the DAG Plan.  Fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``nodes``
     - Mapping from node UUID to ``BasicPlan``.
   * - ``edges``
     - Set of ``(from_node_uuid, to_node_uuid)`` pairs expressing dependencies.
   * - ``inputs``
     - UUIDs of datasets that must be supplied externally before any node can
       start (typically empty for query plans).
   * - ``outputs``
     - UUIDs of the datasets that constitute the final result.

**Suggested source location:**
``src/lunch/query_engines/dag_plan.py``

Base Classes
============

lunch uses a small set of base classes to give every class in the codebase
a clearly defined *role*.  Each role carries strict constraints on what a
class is allowed to do.  The goal is to make the codebase easier to reason
about, test, and extend.

.. contents:: On this page
   :local:
   :depth: 2


Motivation
----------

Traditional object-oriented design encourages grouping state and behaviour
together in the same class.  In an async, distributed system this causes
problems:

- **Testing becomes hard.** A class that holds state *and* transforms data
  requires tests that set up state, call behaviour, and then inspect state.
  Edge cases multiply quickly.
- **Concurrency bugs hide in state.** The more classes hold mutable state,
  the more places a race condition can live.
- **Responsibilities blur.** A class that caches, transforms, *and*
  delegates is doing three jobs.  When something goes wrong it is not
  obvious which job failed.

The four active base classes — ``Transformer``, ``Conductor``, ``Stateful``,
and ``Data`` — enforce a strict separation of these concerns.


The Four Roles
--------------

Transformer
~~~~~~~~~~~

A ``Transformer`` is **stateless** and contains only **static methods**.
It receives data in, returns transformed data out, and has no memory of
previous calls.

.. code-block:: python

    class Transformer:
        @staticmethod
        def reverse(s: str) -> str:
            return s[::-1]

**Rules:**

- No instance variables.
- No calls to ``Stateful`` or ``Conductor`` classes.
- If a cache would speed up a transformation, that cache belongs in a
  ``Conductor`` that wraps the ``Transformer`` — not in the ``Transformer``
  itself.

**Testing:** Input → output assertions, no setup required.

.. code-block:: python

    assert VersionsTransformer.start_new_write_version({}) == {...}

**Example:** ``VersionsTransformer`` computes new MVCC version dicts from
existing ones.  ``DimensionTransformer`` fills in missing ``id_`` and
``storage`` fields on a dimension dict.


Conductor
~~~~~~~~~

A ``Conductor`` is **stateless** except for the references it holds to
other objects.  It delegates all real work to those objects — it performs
**no transformations itself**.

.. code-block:: python

    class ModelManager(Conductor):
        def __init__(self, store, validator, transformer):
            self._store = store
            self._validator = validator
            self._transformer = transformer

        async def update_dimension(self, dim, read_version, write_version):
            return await _update_dimension(
                dim, read_version, write_version,
                self._store, self._validator, self._transformer,
            )

The real logic lives in module-level ``_private_async_functions``.  The
class is a thin delegation layer that wires collaborators together and
routes calls to the right private function.

**Rules:**

- Holds references to other objects; holds no data of its own.
- Makes calls; does not transform.
- Logic lives in private module-level functions, not in methods.

**Testing:** Mock all collaborators; assert the right calls were made with
the right arguments.  Critically, **error handling is a first-class
responsibility of the Conductor** — tests must cover what happens when a
collaborator raises an exception.  A Conductor that silently swallows
errors, re-raises the wrong type, or fails to clean up on error is broken
even if its happy-path tests all pass.

.. code-block:: python

    async def test_update_dimension_propagates_validation_error(mock_store, mock_validator):
        mock_validator.validate.side_effect = DimensionValidationError("bad input")
        with pytest.raises(DimensionValidationError):
            await manager.update_dimension(bad_dim, read_version, write_version)
        mock_store.put_dimensions.assert_not_called()

**Example:** ``ModelManager`` routes ``update_dimension`` and
``update_fact`` calls to validators, transformers, and storage.
``ReferenceDataManager`` delegates dimension import operations to an
optimiser and an enactor.


Stateful
~~~~~~~~

A ``Stateful`` class **holds mutable state**.  Caches and persistors are
``Stateful``.  A ``Stateful`` may call ``Transformer`` classes to prepare
data, but must not contain transformation logic itself.

.. code-block:: python

    class ModelCache(Stateful):
        async def get_dimension(self, id_: int, model_version: int): ...
        async def put_dimension(self, dimension: dict, model_version: int): ...

**Rules:**

- Owns and mutates state.
- May delegate to ``Transformer`` classes; must not contain transformation
  logic itself.
- ``Null*`` implementations (e.g. ``NullModelCache``) are no-ops used when
  caching is disabled or in tests.

**Testing:** Before/after scenarios; may require async correctness tests.
Because ``Stateful`` classes are deliberately few, the test surface for
concurrency bugs is kept small.

**Example:** ``ModelCache`` caches dimension and fact dicts keyed by
model version.  Persistors such as ``LocalFileColumnarDimensionDataPersistor``
own file handles and write state to disk.


Data
~~~~

A ``Data`` class is a **pure data container** with no behaviour.  It
carries structured information between pipeline stages.

.. code-block:: python

    class BasicPlan(Data):
        function_name: str
        inputs: dict
        outputs: dict

**Rules:**

- No methods beyond simple accessors.
- No calls to any other class.

**Example:** ``BasicPlan``, ``SerialPlan``, and ``ParallelPlan`` describe
import execution plans produced by planners and consumed by enactors.  The
plan is pure data; the enactor decides what to do with it.


Constant
~~~~~~~~

A ``Constant`` holds **immutable configuration** shared across the
application.  Unlike ``Data``, a ``Constant`` is not passed through a
pipeline — it is a fixed, application-wide value.

**Rules:**

- Holds only immutable state (no mutation after construction).
- May expose accessor methods to extract parts of the configuration.

**Testing:** Assert that accessors return the expected fixed values.

**Example:** ``GlobalState`` holds the default columnar storage
configuration for dimensions.


How the Roles Fit Together
--------------------------

A typical read path through the storage layer illustrates how the roles
interact::

    Manager (Conductor)
        └─ Store (Conductor)
               ├─ Serializer (Transformer)  ← transforms bytes ↔ objects
               ├─ Persistor (Stateful)      ← reads/writes files or StringIO
               └─ Cache (Stateful)          ← optional in-memory shortcut

- The **Manager** delegates to the **Store**.
- The **Store** checks the **Cache** (``Stateful``); on a miss it asks the
  **Serializer** (``Transformer``) to deserialise data from the
  **Persistor** (``Stateful``).
- No class does more than one job.

The import pipeline follows the same pattern::

    Optimiser (Conductor)
        └─ Planner (Transformer) → Plan (Data)

    Enactor (Conductor)
        └─ consumes Plan (Data)
           └─ calls Store (Conductor) → Persistor (Stateful)


Testing Strategy by Role
------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Role
     - What to test
     - How
   * - ``Transformer``
     - Input → output correctness
     - Plain ``assert`` statements; no mocks needed
   * - ``Conductor``
     - Correct calls to collaborators; correct error handling
     - Mock all collaborators; assert call arguments and that errors from
       collaborators are propagated or handled correctly
   * - ``Stateful``
     - State before and after mutation
     - Before/after assertions; test async correctness
   * - ``Data``
     - Field values; immutability if using pyrsistent
     - Construct and inspect; rarely needs dedicated tests
   * - ``Constant``
     - Accessor correctness
     - ``assert constant.x == expected_value``


Why Not Standard OOP?
---------------------

A conventional OOP class that caches, transforms, *and* delegates is
simultaneously a ``Stateful``, a ``Transformer``, and a ``Conductor``.
Testing it requires constructing real (or deeply mocked) collaborators,
seeding state, exercising behaviour, and then inspecting state — in every
test.

By keeping the roles separate:

- ``Transformer`` tests need only an input and an expected output.
- ``Conductor`` tests mock every collaborator and verify routing only.
- ``Stateful`` tests focus exclusively on state transitions.
- Concurrency bugs are confined to ``Stateful`` classes, which are few and
  clearly identified.

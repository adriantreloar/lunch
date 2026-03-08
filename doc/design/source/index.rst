lunch — Design Documentation
============================

.. warning::

   **This is design documentation.**

   Everything in this documentation describes **intended behaviour that may
   not yet exist in the codebase**.  Do not assume that any class, function,
   module, or interface described here is currently implemented.

   To understand what is implemented today, read the **Reference
   documentation** in ``doc/reference/``.

Design documents are the starting point for new functionality.  The workflow
is:

1. The designer writes or refines a design document here.
2. The human reviews and approves the design.
3. An agent reads the approved design and breaks it into issues in the beads
   issue tracker (``bd create ...``).
4. Issues are worked in the beads workflow: code is written, tests are
   written, and reference documentation is updated to match the new code.
5. If implementation reveals that the design needs adjustment, the design
   document is updated first and a new beads issue is raised for the change.

It is acceptable for content to be duplicated between this build and the
reference build.  The design document is authoritative for **what is
intended**; the reference document is authoritative for **what exists**.

.. toctree::
   :maxdepth: 2
   :caption: Design

   queries
   query_engines
   query_specifier
   query_planner
   query_enactor
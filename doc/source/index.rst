.. lunch documentation master file, created by
   sphinx-quickstart on Thu Apr  7 19:32:12 2022.

Welcome to lunch's documentation!
==================================

**lunch** is an MVCC (Multi-Version Concurrency Control) OLAP cube server.
Data is stored in columnar Parquet files organised by version.
Queries and imports are parallelised via Apache Arrow Flight and Dask.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   data_model
   base_classes
   persistors
   dimension_import
   fact_import
   plans
   queries
   query_engines
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

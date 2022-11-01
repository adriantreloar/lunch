# lunch

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
An MVCC cube server that uses dask for grid computing capability.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traditional cube technologies such as SQl Server Analysis Services (SSAS) 
have slow write speeds, which makes them a poor choice for anything but
reporting applications.

Write speeds can be sped up, and calculations and queries made 
parallelisable by adopting a Multi Version Concurrency Control (MVCC) 
approach to data storage.

Read speeds are sped up by the cache friendly and parallelisable 
nature of MVCC storage.

This project allows me to try out the following technologies
+ Data oriented programming, with total separation of code and state
+ Automatic job generation for a dask grid using dask array
+ Kubernetes for job scaling

Also I can sharpen my knowledge of
+ Pandas and numpy programming
+ Parser generation
+ Parallelised MVCC storage design

Currently the project is an infant state, I haven't quite set up CI/CD, 
and the documentation is almost non-existent.

A functioning MVCC storage layer for cube data **has** been built.
The storage layer is using yaml, but this will be replaced with parquet
for the large data sets.

the plan is to:

+ Use tatsu to parse a query language such as MDX
+ Use the tatsu generated AST to generate instructions for dask to process the query 
+ Use the MVCC metadata layer to direct the dask graph to the correct files 
+ Scale the design using docker and Kubernetes


Plans
=====
A plan is instructions for performing a query or process.
Databases use plans so that they can dynamically create a
data process that uses the available resources efficiently.

Basic Plan
----------
A basic plan is simply the name of a function,
the input parameters for the function, and the output parameters.
Note that the output parameters will have a name and a GUID.
The GUID allows the plan enactor to use the outputs of one process
as the inputs of another process in the chain.

Remote Plan
-----------
A remote plan is similar to a basic plan -
the difference being that that the plan is expected to be
executed remotely, on the stated resource.

It is not envisioned that a remote plan would have sub parts.
It should be up to the remote service to decide how to break down
any plans passed to it.

There are two types of more complex plans.

Serial Plan
-----------
A serial plan is a set of steps to be performed.
The outputs of some steps may form the inputs of other steps.
The loop for the  serial plan should be executed locally, but the plans within
may specify Remote Plans which will (Serially) be sent off for execution.

A serial plan is simply a wrapper around a list of Basic, Remote and other Serial or Parallel plans.

Parallel Plan
-------------
A parallel plan is a wrapper around a set of Plans to be called
in parallel, locally. The call (i.e. loop or asyncio.gather) will be local, but Remote Plans
may be involved.


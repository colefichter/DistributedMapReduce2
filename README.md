Map Reduce Server (MRS) version 2
=================================

This project is a fully distributed version of [DistributedMapReduce](https://github.com/colefichter/DistributedMapReduce). It uses resource discovery within a cluster to operate in a fully peer-to-peer style, rather than using the master/slave configuration of the original project.

This project is a demonstration of how to implement a simple map-reduce system in Erlang/OTP. It's not intended to be used as a production system (indeed, it doesn't even have error handling). The purpose is purely educational.

Erlang lends itself well to such a project for a number of reasons:
* parallelism across physical machines in a cluster comes for free
* the language is functional with higher order functions that can be passed around between processes and machines
* actor model concurrency works very well for this sort of task

Thanks to these and other benefits, the whole working system is in just two source files, currently just over 100 lines of code including comments, and whitespace. A handful of ready-to-run implementations of simple map-reduce algorithms are also available in the "compute.erl" module. 

The source code files
---------------------

There are currently four files in the /src folder:
* bootstrap.erl - some simple util functions to make it easier to get a cluster running.
* compute.erl - contains sample MapReduce algorithms
* mrs.erl - represents a central query coordinator for the distributed system.
* worker.erl - represents a worker process that will store integers and process map commands from the mrs server.

Starting a cluster
------------------

We rely on the technique described in [Erlang and OTP in Action](http://www.manning.com/logan/). First, open two command windows in the root project folder and start 'empty' erlang nodes that don't run any application code by running:

    >start_contact_node.bat contact1

in one command window and:

    >start_contact_node.bat contact2

in the other command window. Now we're ready to start two nodes that operate instances of the MRS application.  Open two more command prompts in the root project folder and run:

    >start_server.bat server1

in one command prompt (wait until it you see the line "Finished waiting for resource discovery" before proceeding) and:

    >start_server.bat server2

in the second command prompt.

You've now got a cluster of peer-to-peer map reduce servers running. Try using the compute funtions to see what happens when you run computations across the cluster:

    (server@localhost)1> compute:sum().

Next, try exploring the source code and writing your own computations!
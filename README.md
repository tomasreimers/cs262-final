# PyShard
Created by Nicholas Mahlangu, Tianyu Liu, and Tomas Reimers.

### Project Abstract
Currently, it is difficult to write a program that uses the power of multiple computers. We discuss potential uses of such a library and introduce PyShard, a Python library for writing multi-computer python programs. Pyshard allows simple distribution of work across a network using simple decorators and library calls. We find that the network time to pass data around is largely negligible (less than 0.3s) compared to the time it takes large workloads to run, and discuss ways which we transmit computation over the wire. We end with a discussion of current limitations, constraints, and possible extensions.

### File Descriptions 
* unit_test.py
    * Some tests showing how the system functions 
* run_worker.py
    * Starts a worker that listens for work to do 
* lib/Nexus.py
    * Main part of the system that ccepts work from the client, dispatches it to workers, monitors worker state, and hands the client results back as they are returned
* lib/Resources.py
    * Defines "work" that a client wants to do and the objects that are sent over-the-wire  
* lib/Worker.py
    * Runs "work" assigned by the Nexus and returns its result to the Nexus

### Demo
Run the demo to see for yourself.

```sh
$ python unit_tests.py
```
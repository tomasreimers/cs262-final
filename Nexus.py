#
# Sharded Library
#   Authors: Tomas, Nick, Tianyu
#
# This library allows you to configure one computer to run a python program with
# the power of multiple computers.
#
# On each of the Workers (auxilary computers), run ShardedWorker.py, please ensure
# the computer is configured with any libraries code that needs to run on it
# will require. The Nexus (main computer that runs the major file), will dispatch
# work to the Workers periodically.
#
# Each Worker will start a server and print the address and port of the
# server, along with a Key that is needed to access the server.
#
# Upon running an async computation (computation), the computation will be added to a
# Queue. When a Worker becomes available, the computation will be converted to a
# runnable, serialized, sent to the worker, and then we will wait for a returned
# from the worker.
#
# The nexus periodically pings workers for a heartbeat, should a worker fail to
# provide one for N cycles, the nexus will assume the worker has died and
# dispatch its work to another worker.
#

#
# GENERAL NOTES:
#  - Seems like picloud was trying to do our project (but commercially) in 2013
#    They've since shut down, but you can read about it here:
#    https://www.quora.com/Can-lambda-functions-and-other-Python-code-be-
#    (Comments of first answer about cloud.call)
#
#  - https://www.quora.com/Distributed-Systems-How-and-when-is-state-machine-replication-useful-in-practice
#


#
# GENERAL NOTE:
#  - Do we need to put locks around everything? Or does the GIL protect thread
#    thread operations.
#


import json
from flask import Flask
import thread
import time
import requests
import Resources

STATE_READY = 1
STATE_RUNNING = 2
STATE_COMPLETE = 3
STATE_DEAD = 4

STATE_STRINGS = {
    STATE_READY: "State ready",
    STATE_RUNNING: "State running",
    STATE_COMPLETE: "State complete"
}

class Nexus(object):
    """
    The main machine running computations, this object allows the user to
    queue computations for workers.
    """

    # Function:
    #     Initialize a Nexus object
    #
    def __init__(self):
        self._workers = []
        self._queued_computations = []

    # Function:
    #     Registers a worker to this nexus
    # Args:
    #     addr (string)     : IP address of the worker
    #     password (string) : password for the worker 
    # Returns:
    #    None
    # addr should be ip_address:port (i.e. 10.10.0.1:1234)
    def register_worker(self, addr, password):
        new_worker = RemoteWorker(self, addr, password)
        self._workers.append(new_worker)

    def load_work(self, computation):
        self._queued_computations.append(computation)

    # Function:
    #     Unloads queued Computation objects to remote worker(s)
    # Args:
    #     None
    # Returns:
    #    None
    def unload_work(self):
        # TODO: needs a lot of error handling. No worker? No job to run?
        for worker in self._workers:
            if worker._state == STATE_READY:
                self.assign_work_to(worker)

    # Function:
    #     Assigns a Computation object to a specific worker. Does nothing
    #     if no queued Computation objects.
    # Args:
    #     remote_worker (instance) : remote worker to send computation object to
    # Returns:
    #    None
    def assign_work_to(self, remote_worker):
        if len(self._queued_computations) == 0:
            return

        # nexus sends Computation objects to workers
        computation = self._queued_computations.pop(0)
        remote_worker.assign_work(computation)


class RemoteWorker(object):
    """The Nexus' interal representation of Workers."""

    # Function:
    #     Initialize a RemoteWorker object
    # Args:
    #     nexus (instance)  : nexus object this worker will "belong" to
    #     addr  (string)    : IP address of the worker
    #     password (string) : password for the worker
    # Returns:
    #    None
    def __init__(self, nexus, addr, password):
        self._state = STATE_READY
        self._running = None
        self._addr = addr
        self._password = password
        self._nexus = nexus

        # Set up self.ping to run every few seconds in a different thread
        thread.start_new_thread(self.ping, ())

    # Function:
    #     Assign work to a remote worker
    # Args:
    #     computation (instance) : Computation object
    # Returns:
    #    TODO
    def assign_work(self, computation):
        self._state = STATE_RUNNING
        self._running = computation

        #
        # TODO : Actually transmit to the remote worker instance
        #
        print computation

        #
        # THIS IS WHERE YOU ACTUALLY CONVERT A COMPUTABLE OBJECT INTO A SERIALIZED
        # SOMETHING. USE JSON FOR NOW
        #

        try:
            res = requests.get(self._addr + "computation", timeout=0.5)
        except requests.exceptions.ConnectionError as e:
            # Encountered issue connecting to worker, log error message and
            # invalidate this worker
            print e
            self._state = STATE_DEAD
            return 1

    def ping(self):
        while True:

            try:
                # Hard coded to ping every 3 seconds
                # TODO: not sure if timeout = 0.1 makes sense
                res = requests.get(self._addr + "heartbeat", timeout=0.5)

            except requests.exceptions.ConnectionError as e:
                # Encountered issue connecting to worker, log error message,
                # kill heartbeat thread and invalidate this worker
                print e
                self._state = STATE_DEAD
                return 1

            # If worker is ready or running, heartbeat is okay
            if res.text == STATE_STRINGS[STATE_READY] or res.text == STATE_STRINGS[STATE_RUNNING]:
                print "heartbeat alive"
                pass
            else:
                # Othersiew, the worker should be retuning a result from computatoin.
                # Try to deserialize data
                # TODO: Definately needs error handling here.
                returned = Resources.Returned.unserialize(res.text)
                self._running.done(returned)
            time.sleep(1)

        #
        # TODO : Ping the server every few seconds for status, the server sends back
        #        STATE_READY, STATE_RUNNING, STATE_COMPLETE -- should it be complete,
        #        it should also send back the returned and switch its own state to ready.
        #        When this returns STATE_READY or STATE_COMPLETE it should tell the
        #        nexus to assign it more work. The pings will need to happen in a
        #        separate thread -- consider setting up a thread pool for all the
        #        RemoteWorker objects on the nexus.
        #
        #        (https://docs.python.org/2/library/multiprocessing.html)
        #
        #        P.s. also consider thread safety
        #

        pass
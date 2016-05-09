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

import json
from flask import Flask
import threading
import time
import requests
import Resources
import threading
import functools
import Resources

WORKER_SLEEP_TIME = 1 # in seconds

STATE_READY = 1
STATE_RUNNING = 2
STATE_COMPLETE = 3
STATE_DEAD = 4

STATE_STRINGS = {
    STATE_READY: "State ready",
    STATE_RUNNING: "State running",
    STATE_COMPLETE: "State complete"
}

#
# NOTE: Thanks to the GIL, we can actually ignore what would otherwise be common
#       sync issues.
#

class Nexus(object):
    """
    The main machine running computations, this object allows the user to
    queue computations for workers.
    """

    # Function:
    #     Initialize a Nexus object
    #
    def __init__(self):
        #
        # TODO : what if there are no workers?
        #

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
    def register_worker(self, addr, password, sleep_time=WORKER_SLEEP_TIME):
        new_worker = RemoteWorker(self, addr, password, sleep_time=sleep_time)
        self._workers.append(new_worker)

    def load_work(self, computation):
        self._queued_computations.append(computation)
        self.unload_work()

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

    def all_done(self):
        # check if anything left
        if len(self._queued_computations) > 0:
            return False

        # check if workers still going
        for worker in self._workers:
            if worker._state == STATE_RUNNING:
                return False
        return True

    def wait(self):
        while not self.all_done():
            time.sleep(WORKER_SLEEP_TIME)

    def shard(self, f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            runnable = Resources.Runnable(f, args, kwargs)
    	    computation = Resources.Computation(runnable)
            self.load_work(computation)
            return computation

        return wrapped


class RemoteWorker(object):
    """The Nexus' interal representation of Workers."""

    # Function:
    #     Initialize a RemoteWorker object
    # Args:
    #     nexus (instance)  : nexus object this worker will "belong" to
    #     addr  (string)    : web address of the worker
    #     password (string) : password for the worker
    # Returns:
    #    None
    def __init__(self, nexus, addr, password, sleep_time=WORKER_SLEEP_TIME):
        self._state = STATE_READY
        self._running = None
        self._addr = addr
        self._password = password
        self._nexus = nexus
        self._sleep_time = sleep_time

        # Set up self.ping to run every few seconds in a different thread
        self._thread = threading.Thread(target=self.ping)
        self._thread.daemon = True
        self._thread.start()

    # Function:
    #     Assign work to a remote worker. Currently serializes object using JSON
    # Args:
    #     computation (instance) : Computation object
    # Returns:
    #    TODO
    def assign_work(self, computation):
        assert(self._state == STATE_READY)

        self._state = STATE_RUNNING
        self._running = computation

        print "func. assign_work log | About to assign runnable to worker"
        try:
            # send get request with data
            # print "func. assign_work log | computation._runnable.serialize(): ", computation._runnable.serialize()
            res = requests.get(self._addr + "computation", data=computation._runnable.serialize())
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

                # NOTE: It is a design decision not to remove the nexus' worker
                #       list. It is up to the nexus to reap zombies
                #       appropriately, we do this in case the nexus would like
                #       to run any manual cleanup (e.g. spin up another AWS
                #       box).

                self._state = STATE_DEAD

                # Return computation back to nexus' queue
                if self._running is not None:
                    self._nexus.load_work(self._running)

                # end the thread
                return 1

            # If worker is ready or running, heartbeat is okay
            worker_response = json.loads(res.text)
            if worker_response['status_code'] == STATE_READY or worker_response['status_code'] == STATE_RUNNING:
                print "heartbeat alive"
                self.state = worker_response['status_code']
            elif worker_response['status_code'] == STATE_COMPLETE:
                # Otherwise, the worker should be retuning a result from computatoin.
                # Try to deserialize data
                # TODO: Definately needs error handling here.
                print "func. ping log | worker returned a result"
                returned = Resources.Returned.unserialize(worker_response['result'])
                self._running.done(returned)
                self._running = None
                self._state = STATE_READY
                self._nexus.assign_work_to(self)
                # continue so we can start work immediately
                continue
            else:
                assert(False, "State code unrecognized")

            # sleep before we query again
            time.sleep(self._sleep_time)

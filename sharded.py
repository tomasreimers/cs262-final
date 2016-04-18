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


STATE_READY = 1
STATE_RUNNING = 2
STATE_COMPLETE = 3


class Nexus(object):
    """
    The main machine running computations, this object allows the user to
    queue computations for workers.
    """

    def __init__(self):
        self._workers = []
        self._queued_runnables = []

    # addr should be ip_address:port (i.e. 10.10.0.1:1234)
    def register_worker(self, addr, password):
        new_worker = RemoteWorker(self, addr, password)
        self._workers.append(new_worker)

    def unload_work(self):
        for worker in self._workers:
            if worker._state == STATE_READY:
                self.assign_work_to(worker)

    def assign_work_to(self, remote_worker):
        if len(self._queued_runnables) == 0:
            return

        computation = self._queued_runnables.pop(0)
        remote_worker.assign_work(computation)


class RemoteWorker(object):
    """The Nexus' interal representation of Workers."""

    def __init__(self, nexus, addr, password):
        self._state = STATE_READY
        self._running = None
        self._addr = addr
        self._password = password
        self._nexus = nexus

        #
        # TODO set up self.ping to run every few seconds in a different thread
        #

    def assign_work(self, computation):
        self._state = STATE_RUNNING
        self._running = computation

        #
        # TODO : Actually transmit to the remote worker instance
        #

    def ping(self):

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

class Worker(object):
    """The remote server, this accepts work from a nexus and runs it."""

    def __init__(self):

        #
        # TODO : Should start a threadpool to execute tasks in, then call
        #        self.start()
        #

        pass

    def start(self):

        #
        # TODO : should start a flask server a print the location / password.
        #        the flask server recieves requests (either pings (status
        #        updates) or actual requests for computation -- NOTE:
        #        computation _MUST_ happen in a separate thread)
        #
        # NOTE : this generates a password (random string), reject any requests
        #        that lack the password.
        #

        pass

    # http://lybniz2.sourceforge.net/safeeval.html -- consider using this to pass
    # globals / locals found in f.func_globals


class Computation(object):
    """
    The local representation of work that needs to be run / has been run.
    It resembles a promise in JS.
    """

    def __init__(self, runnable):
        self._runnable = runnable
        self._returned = None
        self._state = STATE_READY
        self._callbacks = []

    def then(self, callback):
        if (self._state == STATE_COMPLETE):
            callback(self._returned)
        else:
            self._callbacks.append(callback)

    def done(self, returned):
        self._returned = returned
        self._state = STATE_COMPLETE

        while (len(self._callbacks) > 0):
            f = self._callbacks.pop(0)
            f(returned)

#
# NOTE: Currently runnables expect the worker to have the correct packages
#       installed, and that the function doesn't modify / depend on globals or
#       lexical scope (may consider changing the last two)
#

class Runnable(object):
    """A serializable representation of work to run."""

    def __init__(self, f_code=""):
        #
        # TODO : Think of some way to represent args
        #
        self._f_code = f_code

    def serialize(self):
        #
        # TODO : replace with Protobufs
        #
        return json.dumps({
            'f_code': self._f_code,
        })

    @classmethod
    def from_function(cls, f):
        #
        # TODO : find some way to get code from function -- look into how
        #        cloudpickle does it (https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/cloudpickle.py)
        #
        return cls("# PLEASE IMPLEMENT")

    @classmethod
    def unserialize(cls, value):
        unserialized = json.parse(unserialized_values)
        return cls(f_code=unserialized['f_code'])


class Retured(object):
    """A serializable of work that ran."""

    def __init__(self, is_exception=False, value=None):
        self._is_exception = is_exception
        self._value = value

    def serialize(self):
        #
        # TODO : replace with Protobufs, also this will fail is self._value is an exception
        #
        return json.dumps({
            'is_exception': self._is_exception,
            'value': self._value
        })

    @classmethod
    def unserialize(cls, value):
        unserialized = json.parse(unserialized_values)
        return cls(is_exception=unserialized['is_exception'], value=unserialized['value'])

import json
from flask import Flask
import thread
import time
import requests
import dill
import inspect
import base64

STATE_READY = 1
STATE_RUNNING = 2
STATE_COMPLETE = 3
STATE_DEAD = 4

STATE_STRINGS = {
    STATE_READY: "State ready",
    STATE_RUNNING: "State running",
    STATE_COMPLETE: "State complete"
}

class Computation(object):
    """
    The local representation of work that needs to be run / has been run.
    It resembles a promise in JS.
    """

    def __init__(self, runnable):
        """
        Function:
            Initailizes a Computation object

        """
        self._runnable = runnable
        self._returned = None
        self._state = STATE_READY
        self._callbacks = []

    # this is executed by the nexus
    # Function:
    #     Execute / prepare execution of a callback on the result of a computation
    # Args:
    #     callback (function) : function to be executed on the result of a computation
    # Returns:
    #     None
    def then(self, callback):
        if (self._state == STATE_COMPLETE):
            callback(self._returned)
        else:
            self._callbacks.append(callback)

    # Function:
    #
    # Args:
    #
    # Returns:
    #
    def done(self, returned):
        self._returned = returned
        self._state = STATE_COMPLETE

        while (len(self._callbacks) > 0):
            f = self._callbacks.pop(0)
            f(returned)

class Runnable(object):
    """
    A serializable representation of work to run.

    Assumptions:
        - Expect the worker to have the correct packages installed
        - Function doesn't modify / depend on globals or lexical scope
    """

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    def __init__(self, f_code, f_args=None, f_kwargs=None):
        """
        Initializes a Runnable object

        Args:
            f_code (function) : function to be remote executed
            f_args (array)    : arguments to function that should be executed

        Returns:
            Nothing.
        """
        if f_args is None:
            f_args = []

        if f_kwargs is None:
            f_kwargs = {}

        self._f_code = f_code
        self._f_args = f_args
        self._f_kwargs = f_kwargs

    # Function:
    #     Create a serialized string to send to remote worker
    # Args:
    #     TODO
    # Returns:
    #    string
    def serialize(self):
        # TODO : remove this and use below
        # TODO : replace with Protobufs
        dilled = base64.b64encode(dill.dumps(self._f_code))

        return json.dumps({
            'f_args': self._f_args,
            'f_kwargs': self._f_kwargs,
            'f_code': dilled
        })

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    @classmethod
    def unserialize(cls, value):
        unserialized = json.loads(value)
        return cls(
            f_code=dill.loads(base64.b64decode(unserialized['f_code'])),
            f_args=unserialized['f_args'],
            f_kwargs=unserialized['f_kwargs']
        )

    def evaluate(self):
        return self._f_code(*self._f_args, **self._f_kwargs)


# worker sends this back to the nexus
class Returned(object):
    """A serializable of work that ran."""

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    def __init__(self, is_exception=False, value=None):
        self._is_exception = is_exception
        self._value = value

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    def serialize(self):
        # TODO : replace with Protobufs, also this will fail is self._value is an exception
        return json.dumps({
            'is_exception': self._is_exception,
            'value': self._value
        })

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    @classmethod
    def unserialize(cls, value):
        unserialized = json.loads(value)
        return cls(is_exception=unserialized['is_exception'], value=unserialized['value'])

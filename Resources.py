import json
from flask import Flask
import thread
import time
import requests

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
    #    None
    def then(self, callback):
        if (self._state == STATE_COMPLETE):
            callback(self._returned)
        else:
            self._callbacks.append(callback)

    # Function:
    #     Execute / prepare execution of a callback on the result of a computation
    # Args:
    #     callback (function) : function to be executed on the result of a computation
    # Returns:
    #    None
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
    def __init__(self, f_code, f_args=[]):
        """
        Initializes a Runnable object

        Args:
            f_code (function) : function to be remote executed
            f_args (array)    : arguments to function that should be executed

        Returns:
            Nothing.
        """
        self._f_code = f_code
        self._f_args = f_args

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    def serialize(self):
        #
        # TODO : replace with Protobufs
        #
        return json.dumps({
            'f_code': self._f_code,
        })

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    @classmethod
    def from_function(cls, f):
        #
        # TODO : find some way to get code from function -- look into how
        #        cloudpickle does it (https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/cloudpickle.py)
        #
        return cls("# PLEASE IMPLEMENT")

    # Function:
    #     TODO
    # Args:
    #     TODO
    # Returns:
    #    TODO
    @classmethod
    def unserialize(cls, value):
        unserialized = json.parse(unserialized_values)
        return cls(f_code=unserialized['f_code'])


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
        #
        # TODO : replace with Protobufs, also this will fail is self._value is an exception
        #
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

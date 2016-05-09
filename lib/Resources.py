import json
from flask import Flask
import thread
import time
import requests
import inspect

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

    """
    Function:
        Execute / prepare execution of a callback on the result of a computation
    Args:
        callback (function) : function to be executed on the result of a computation
    Returns:
        None
    """
    def then(self, callback):
        if (self._state == STATE_COMPLETE):
            callback(self._returned)
        else:
            self._callbacks.append(callback)

    """
    Function:
        Execute callback on the result of a computation
    Args:
        returned (Returned) : result of computation, used as argument for the callback
    Returns:
        None
    """
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

    def __init__(self, f_code, f_args=[]):
        """
        Initializes a Runnable object

        Args:
            f_code (function) : function to be remote executed
            f_args (array)    : arguments to function that should be executed

        Returns:
            None
        """
        self._f_code = f_code
        self._f_args = f_args

    """
    Function:
        Serialize the Runnable object into a string, currently using json
    Args:
        None
    Returns:
        String
    """
    def serialize(self):
        return json.dumps({
            'f_code': inspect.getsource(self._f_code),
            'f_args': self._f_args
        })

    """
    Function:
        TODO
    Args:
        TODO
    Returns:
        TODO
    """
    @classmethod
    def from_function(cls, f):
        #
        # TODO : find some way to get code from function -- look into how
        #        cloudpickle does it (https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/cloudpickle.py)
        #
        return cls("# PLEASE IMPLEMENT")

    """
    Function:
        Unserialize a string and create an instance of Runnable object
    Args:
        value (String) : string of a serialized Runnable object
    Returns:
        Runnable
    """
    @classmethod
    def unserialize(cls, value):
        unserialized = json.parse(value)
        return cls(f_code=unserialized['f_code'])


# worker sends this back to the nexus
class Returned(object):
    """A serializable of work that ran."""

    """
    Initializes a Returned object

    Args:
        is_exception (Bool) : flag for exception when the worker fails a computation job
        value (Object) : computation result from Worker

    Returns:
        None
    """

    def __init__(self, is_exception=False, value=None):
        self._is_exception = is_exception
        self._value = value

    """
    Function:
        Serialize the Returned object into a string, currently using json
    Args:
        None
    Returns:
        String
    """
    def serialize(self):
        # TODO : replace with Protobufs, also this will fail is self._value is an exception
        return json.dumps({
            'is_exception': self._is_exception,
            'value': self._value
        })

    """
    Function:
        Unserialize a string and create an instance of Returned object
    Args:
        value (String) : string of a serialized Returned object
    Returns:
        Returned
    """
    @classmethod
    def unserialize(cls, value):
        unserialized = json.loads(value)
        return cls(is_exception=unserialized['is_exception'], value=unserialized['value'])

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


class Returned(object):
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
        unserialized = json.loads(value)
        return cls(is_exception=unserialized['is_exception'], value=unserialized['value'])

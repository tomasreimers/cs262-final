import json
from flask import Flask
from flask import request
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

class Worker(object):
    """The remote server, this accepts work from a nexus and runs it."""

    def __init__(self, password=None):
        self.state = 0
        self.result = None
        self.password = password

        # flask
        self.app = Flask(__name__)
        self.app.add_url_rule('/<action>', view_func=self.heartbeat_handler, methods=["GET"])


    """
    Handles computation requests and periodic pings from master.
    When a computation job is finished, heartbeat will also return the result.
    When a new computation comes in, heartbeat_handler will dispatch the job and
    mark worker as running.
    Otherwise, heartbeat just reports worker's status to master.
    """
    def heartbeat_handler(self, action):
        if self.password is not None:
            if request.args.get('password') != self.password:
                return "Invalid auth."

        if action == "heartbeat":
            # If worker has completed a job, return self.result with this
            # heartbeat. Otherwise, report self.state.
            if self.state == STATE_COMPLETE:
                # reset worker state to ready
                self.state = STATE_READY
                print "Return result, resetting state to ready"
                return json.dumps({
                    'status_code': STATE_COMPLETE,
                    'status_text': STATE_STRINGS[STATE_COMPLETE],
                    'result': self.result
                })
            else:
                print "Heartbeat at " + STATE_STRINGS[self.state]
                return json.dumps({
                    'status_code': self.state,
                    'status_text': STATE_STRINGS[self.state]
                })

        elif action == "computation":
            print "Incoming job"
            # Initial computation request. Only accept when worker is free.
            if (self.state == STATE_RUNNING):
                return "Worker busy"
            # Dispatch a new thread with to do computation
            else:
                print "About to start computation"
                thread.start_new_thread(self.do_computation, (request.data, ))
                return "Job starts running"
        else:
            return 'Unsupported action'

    """
    Function:
        Wrapper function to run actual computations.
    Args:
        runnable_string (string) : data to be deserialized
    Returns:
        None

    Note:
        - This breaks if you pass a function inside of a class
    """
    def do_computation(self, runnable_string):
        assert(self.state == STATE_READY)

        # unserialize data
        self.state = STATE_RUNNING
        print "func. do_computation log | Runnable string: ", runnable_string
        runnable = Resources.Runnable.unserialize(runnable_string)

        # compute result
        try:
            f_result = runnable.evaluate()
            f_result = Resources.Returned(value=f_result)
        except Exception as e:
            f_result = Resources.Returned(value=e, is_exception=True)

        # update state
        self.result = f_result.serialize()
        self.state = STATE_COMPLETE
        print "Job complete"

    def start(self):
        self.state = STATE_READY

        print "Worker starts"
        self.app.run(threaded = True)

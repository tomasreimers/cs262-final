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
        Creates a string function call
    Args:
        f_name (string) : name of the function
        f_args (list)   : arguments for the function call
    Returns:
        string
    """
    def create_function_call(self, f_name, f_args):
        f_call = f_name + "("
        for f_arg in f_args:
            if isinstance(f_arg, unicode) or isinstance(f_arg, str):
                if f_call[-1] == "(":
                    f_call += "'%s'" % (f_arg)
                else:
                    f_call += ",'%s'" % (f_arg)
            else:
                if f_call[-1] == "(":
                    f_call += str(f_arg)
                else:
                    f_call += "," + str(f_arg)
        f_call += ")"
        return f_call

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
        # unserialize data
        self.state = STATE_RUNNING
        print "func. do_computation log | Runnable string: ", runnable_string
        f_data = json.loads(runnable_string)
        f_args = f_data["f_args"]
        f_code = f_data["f_code"]
        f_name = f_code.split("\n")[0].split(" ")[1].split("(")[0]
        print "func. do_computation log | Got these f_args from the client: ", f_args
        print "func. do_computation log | Got this f_code from the client: \n", f_code
        print "func. do_computation log | Extracted this function name from f_code: ", f_name

        # compute result
        exec(f_code)
        f_result = eval(self.create_function_call(f_name, f_args))
        print "func. do_computation log | Computed result: ", f_result

        # update state
        f_result = Resources.Returned(value=f_result)
        self.result = f_result.serialize()
        self.state = STATE_COMPLETE
        print "Job complete"

    def start(self):
        self.state = STATE_READY

        print "Worker starts"
        self.app.run(threaded = True)
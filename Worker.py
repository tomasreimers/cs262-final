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

    def __init__(self):
        self.app = Flask(__name__)
        self.app.add_url_rule('/<action>', view_func=self.heartbeat_handler, methods=["GET"])

        self.state = 0
        self.result = None

        #
        # TODO : Should start a threadpool to execute tasks in, then call
        #        self.start()
        #

    """
    Handles computation requests and periodic pings from master.
    When a computation job is finished, heartbeat will also return the result.
    When a new computation comes in, heartbeat_handler will dispatch the job and
    mark worker as running.
    Otherwise, heartbeat just reports worker's status to master.
    """
    def heartbeat_handler(self, action):
        if action == "heartbeat":
            # If worker has completed a job, return self.result with this
            # heartbeat. Otherwise, report self.state.

            #
            # TODO : Need to think more on how state changes after a job is done.
            # Might be helpful to add STATE_RETURNED.
            #
            if self.state == STATE_COMPLETE:
                print "Return result"
                return self.result
            else:
                print "Heartbeat at " + STATE_STRINGS[self.state]
                return STATE_STRINGS[self.state]

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

    def create_function_call(self, f_name, f_args):
        f_call = f_name + "(" + ",".join(map(lambda x: str(x), f_args)) + ")"
        return f_call

    """
    Function:
        Wrapper function to run actual computations.
    Args:
        runnable_string (string) : data to be deserialized
    Returns:
       None

    TODO: 
        Unserialize runnable_string, create a Runnable instance and
        execute in current thread. Also need to sync updates to self.state
        and self.result
    """
    def do_computation(self, runnable_string):
        # deserialize data
        self.state = STATE_RUNNING
        f_data = json.loads(runnable_string)
        f_args = f_data["f_args"]
        f_code = f_data["f_code"]
        f_name = f_code.split("\n")[0].split(" ")[1].split("(")[0]
        print "do_computation log | Got these f_args from the client: ", f_args
        print "do_computation log | Got this f_code from the client: \n", f_code
        print "do_computation log | Extracted this function name from f_code: ", f_name

        # compute result
        exec(f_code)
        exec("f_result = " + self.create_function_call(f_name, f_args))
        print "do_computation log | Computed result: ", f_result

        # update state
        dummy_result = Resources.Returned(value="Dummy result")
        self.result = dummy_result.serialize()
        self.state = STATE_COMPLETE
        print "Job complete"

    def start(self):
        self.state = STATE_READY

        #
        # NOTE: May or may not be necessary to set threaded = True, since the request
        # handler could just be a single thread
        #
        print "Worker starts"
        self.app.run(threaded = True)

        #
        # TODO : should start a flask server a print the location / password.
        #        the flask server recieves requests (either pings (status
        #        updates) or actual requests for computation -- NOTE:
        #        computation _MUST_ happen in a separate thread)
        #
        # NOTE : this generates a password (random string), reject any requests
        #        that lack the password.
        #

    # http://lybniz2.sourceforge.net/safeeval.html -- consider using this to pass
    # globals / locals found in f.func_globals
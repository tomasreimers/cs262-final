import Nexus
import Resources
import time

# .then method for nexus
def print_result(result):
    print "*** Result of computation: ", result._value, 'Time:', str(time.time())

"""
Function:
    Counts how many times letter ch appears in string s
Args:
    s (string) - a string to count occurrences in
    ch (char)  - a letter to count occurrences of
Returns:
    int
"""
def nap(d, s):
    time.sleep(d)
    return 1

class TestRemoteFunctionExecution():
    """
    How to run:
        1) execute python run_worker.py in a terminal window
        2) execute python nick-master-test.py in another terminal window
        3) check logs to see if computed result matches expected result
    """

    def set_up(self):
        self.nexus = Nexus.Nexus()
        self.nexus.register_worker("http://localhost:5000/", "")

    """
    Function call:
        nap("apple apple apple apple", "a")
    Expected print value:
        4
    """
    def test_1(self):
        print "Running test #1"

        arg = [7, "a" * 100000000]
        # prepare computation object being sent to client
        runnable = Resources.Runnable(nap, arg)
        computation = Resources.Computation(runnable)
        computation.then(print_result)

        # send work to a remote machine
        print 'Loding computation', 'Time:', str(time.time())
        self.nexus.load_work(computation)
        # print 'Unloding work' + str(time.time())
        self.nexus.unload_work()
        time.sleep(10)

if __name__ == '__main__':
    tester = TestRemoteFunctionExecution()
    tester.set_up()
    tester.test_1()

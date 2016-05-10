from lib import Nexus
from lib import Worker
from lib import Resources
from mock import Mock
from multiprocessing import Process

import unittest
import requests
import time

"""
Function:
    calculate n-factorial
Args:
    n (int): number to calculate
Returns:
    int: n-factorial result
"""
def factorial(n):
    # error checking
    if n < 0:
        return -1
    elif n == 0:
        return 1
    else:
        to_return = 1
        for i in xrange(1, n + 1):
            to_return *= i
        return to_return

"""
Function:
    Zero division, should raise an exception on the worker
Args:
    None
Returns:
    None
"""
def divide_zero():
    return 1 / 0

"""
Function:
    Wrapper function which returns a callback function. The callback function prints
    computation result to the console. Mainly used for debugging purpose
Args:
    name (String): name of the computation job
Returns:
    wrapped (Function): callback function to use with then
"""
def output_of(name):

    def wrapped(result):
        print name, ":", result._value

    return wrapped

"""
Function:
    Wrapper function which returns a callback function. The callback function checks
    computation result
Args:
    value (Object): expected result of the computation job
Returns:
    wrapped (Function): callback function to use with then
"""
def expect_result(value):
    mock_handler = Mock(return_value=None)

    def wrapped(result):
        mock_handler(result._value)
        mock_handler.assert_called_once_with(value)

    return wrapped


class TestWorkerStates(unittest.TestCase):
    """
    Spawn worker process and init nexus
    """
    @classmethod
    def setUpClass(cls):

        # Worker password is set to be foo
        worker = Worker.Worker("foo")
        cls.workerProcess = Process(target=worker.start, args=())
        cls.workerProcess.start()

        cls.nexus = Nexus.Nexus()
        cls.nexus.register_worker("http://localhost:5000/", "foo")

    """
    Wrong password should lead to failed computation (callback should not be called)
    """
    def test_wrong_password(self):
        print "Running wrong passowrd test"
        # Create a Nexus with wrong password
        dummy_nexus = Nexus.Nexus()
        dummy_nexus.register_worker("http://localhost:5000/", "bar")

        sfactorial = dummy_nexus.shard(factorial)

        mock_callback = Mock(return_value=None)

        sfactorial(3).then(mock_callback)

        # Since the computation will not be executed with wrong passowrd,
        # manually sleep rather than using nexus.sleep
        time.sleep(2)

        self.assertFalse(mock_callback.called)

    """
    Computation with correct password should run sucessfullly
    """
    def test_correct_password(self):
        print "Running correct passowrd test"
        sfactorial = self.nexus.shard(factorial)

        mock_callback = Mock(return_value=None)

        sfactorial(3).then(mock_callback)

        self.nexus.wait()
        self.assertTrue(mock_callback.called)

    """
    Valid computation request should return correct result
    """
    def test_valid_computation(self):
      print "Running valid computation test"

      sfactorial = self.nexus.shard(factorial)

      sfactorial(4).then(expect_result(24))
      sfactorial(5).then(expect_result(120))
      sfactorial(6).then(expect_result(720))

      self.nexus.wait()

    """
    Should automatically shard a function with decorator
    """
    def test_function_decorator(self):
        print "Running function decorator test"

        @self.nexus.shard
        def add_five(foo):
            return foo + 5

        mock_callback = Mock(return_value=None)

        self.assertFalse(mock_callback.called)
        self.nexus.wait()

    """
    Computation with error should report exception in Returned
    """
    def test_exception(self):
        print "Running worker exception test"
        sdivide_zero = self.nexus.shard(divide_zero)

        mock_handler = Mock(return_value=None)

        # Callback to test result is exception
        def exception_test(result):
            mock_handler(result._is_exception)
            mock_handler.assert_called_once_with(True)

        result = sdivide_zero().then(exception_test)

        self.nexus.wait()

    """
    Kill the worker process
    """
    @classmethod
    def tearDownClass(cls):
        print "Test complete, terminate worker"
        cls.workerProcess.terminate()
        cls.workerProcess.join()

if __name__ == '__main__':
    unittest.main()

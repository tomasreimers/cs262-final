import Nexus
import Worker
import Resources
import unittest
import requests
import time
from multiprocessing import Process

# .then method for nexus
def print_result(result):
	print "*** Result of computation: ", result._value

def get_value_from_result(result):
	return result._value

"""
Function:
	Returns the sum of two integers
Args:
	a (int) - a number for adding
	b (int) - a number for adding
Returns:
	int
"""
def sum_numbers(a, b):
	return a + b

"""
Function:
	Counts how many times letter ch appears in string s
Args:
	s (string) - a string to count occurrences in
	ch (char)  - a letter to count occurrences of
Returns:
	int
"""
def count_occurrences(s, ch):
	count = 0
	for c in s:
		if c == ch:
			count += 1
	return count

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
		sum(1,2)

	Expected print value:
		3
	""" 
	def test_1(self):
		print "Running test #1"

		# prepare computation object being sent to client
		runnable = Resources.Runnable(sum_numbers, [1,2])
		computation = Resources.Computation(runnable)
		computation.then(print_result)

		# send work to a remote machine
		self.nexus.load_work(computation)
		self.nexus.unload_work()
		time.sleep(5)

	"""
	Function call:
		count_occurrences("apple apple apple apple", "a")
	Expected print value:
		4
	"""
	def test_2(self):
		print "Running test #2"

		# prepare computation object being sent to client
		runnable = Resources.Runnable(count_occurrences, ["apple apple apple apple", "a"])
		computation = Resources.Computation(runnable)
		computation.then(print_result)

		# send work to a remote machine
		self.nexus.load_work(computation)
		self.nexus.unload_work()
		time.sleep(5)

if __name__ == '__main__':
	tester = TestRemoteFunctionExecution()
	tester.set_up()
	tester.test_1()
	tester.test_2()